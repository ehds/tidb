// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"
	"log"
	"math"
	"sort"
	"time"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"google.golang.org/grpc"
)

type joinReorderGreedySolver struct {
	*baseSingleGroupJoinOrderSolver
	eqEdges []*expression.ScalarFunction
}

// solve reorders the join nodes in the group based on a greedy algorithm.
//
// For each node having a join equal condition with the current join tree in
// the group, calculate the cumulative join cost of that node and the join
// tree, choose the node with the smallest cumulative cost to join with the
// current join tree.
//
// cumulative join cost = CumCount(lhs) + CumCount(rhs) + RowCount(join)
//   For base node, its CumCount equals to the sum of the count of its subtree.
//   See baseNodeCumCost for more details.
// TODO: this formula can be changed to real physical cost in future.
//
// For the nodes and join trees which don't have a join equal condition to
// connect them, we make a bushy join tree to do the cartesian joins finally.
func (s *joinReorderGreedySolver) solve(joinNodePlans []LogicalPlan) (LogicalPlan, error) {
	for _, node := range joinNodePlans {
		_, err := node.recursiveDeriveStats(nil)
		if err != nil {
			return nil, err
		}
		s.curJoinGroup = append(s.curJoinGroup, &jrNode{
			p:       node,
			cumCost: s.baseNodeCumCost(node),
		})
	}
	sort.SliceStable(s.curJoinGroup, func(i, j int) bool {
		return s.curJoinGroup[i].cumCost < s.curJoinGroup[j].cumCost
	})

	var cartesianGroup []LogicalPlan
	for len(s.curJoinGroup) > 0 {
		newNode, err := s.constructConnectedJoinTree()
		if err != nil {
			return nil, err
		}
		cartesianGroup = append(cartesianGroup, newNode.p)
	}

	return s.makeBushyJoin(cartesianGroup), nil
}

func (s *joinReorderGreedySolver) constructConnectedJoinTree() (*jrNode, error) {
	curJoinTree := s.curJoinGroup[0]
	s.curJoinGroup = s.curJoinGroup[1:]
	for {
		bestCost := math.MaxFloat64
		bestIdx := -1
		var finalRemainOthers []expression.Expression
		var bestJoin LogicalPlan
		var testActions []LogicalPlan
		for i, node := range s.curJoinGroup {
			newJoin, remainOthers := s.checkConnectionAndMakeJoin(curJoinTree.p, node.p)
			testActions = append(testActions, node.p)
			s.getAction(curJoinTree.p, testActions)
			if newJoin == nil {
				continue
			}
			_, err := newJoin.recursiveDeriveStats(nil)
			if err != nil {
				return nil, err
			}
			curCost := s.calcJoinCumCost(newJoin, curJoinTree, node)
			if bestCost > curCost {
				bestCost = curCost
				bestJoin = newJoin
				bestIdx = i
				finalRemainOthers = remainOthers
			}
		}
		// If we could find more join node, meaning that the sub connected graph have been totally explored.
		if bestJoin == nil {
			break
		}
		curJoinTree = &jrNode{
			p:       bestJoin,
			cumCost: bestCost,
		}
		s.curJoinGroup = append(s.curJoinGroup[:bestIdx], s.curJoinGroup[bestIdx+1:]...)
		s.otherConds = finalRemainOthers
	}
	return curJoinTree, nil
}

func (s *joinReorderGreedySolver) checkConnectionAndMakeJoin(leftNode, rightNode LogicalPlan) (LogicalPlan, []expression.Expression) {
	var usedEdges []*expression.ScalarFunction
	remainOtherConds := make([]expression.Expression, len(s.otherConds))
	copy(remainOtherConds, s.otherConds)
	for _, edge := range s.eqEdges {
		lCol := edge.GetArgs()[0].(*expression.Column)
		rCol := edge.GetArgs()[1].(*expression.Column)
		if leftNode.Schema().Contains(lCol) && rightNode.Schema().Contains(rCol) {
			usedEdges = append(usedEdges, edge)
		} else if rightNode.Schema().Contains(lCol) && leftNode.Schema().Contains(rCol) {
			newSf := expression.NewFunctionInternal(s.ctx, ast.EQ, edge.GetType(), rCol, lCol).(*expression.ScalarFunction)
			usedEdges = append(usedEdges, newSf)
		}
	}
	if len(usedEdges) == 0 {
		return nil, nil
	}
	var otherConds []expression.Expression
	mergedSchema := expression.MergeSchema(leftNode.Schema(), rightNode.Schema())
	remainOtherConds, otherConds = expression.FilterOutInPlace(remainOtherConds, func(expr expression.Expression) bool {
		return expression.ExprFromSchema(expr, mergedSchema)
	})
	return s.newJoinWithEdges(leftNode, rightNode, usedEdges, otherConds), remainOtherConds
}

func encodeLogicalPlan(p LogicalPlan) *LogicalJoinNode {
	var joinNode = LogicalJoinNode{}
	joinNode.Tp = p.TP()
	for _, node := range p.Children() {
		joinNode.Childrens = append(joinNode.Childrens, encodeLogicalPlan(node))
	}
	if join, isJoin := p.(*LogicalJoin); isJoin {
		for _, condition := range join.EqualConditions {
			var args []string
			funcname := condition.FuncName.String()
			for _, arg := range condition.GetArgs() {
				args = append(args, arg.String())
			}
			joinNode.Conditions = append(joinNode.Conditions, &LogicalJoinNode_Condition{Funcname: funcname, Args: args})
		}
	}
	if dataSource, isDataSource := p.(*DataSource); isDataSource {
		for _, condition := range dataSource.allConds {
			if scalarFunc, isScalar := condition.(*expression.ScalarFunction); isScalar {
				var args []string
				funcname := scalarFunc.FuncName.String()
				for _, arg := range scalarFunc.GetArgs() {
					args = append(args, arg.String())
				}
				joinNode.Conditions = append(joinNode.Conditions, &LogicalJoinNode_Condition{Funcname: funcname, Args: args})
			}
		}
	}
	return &joinNode
}
func (s *joinReorderGreedySolver) getAction(curJoinTree LogicalPlan, actions []LogicalPlan) (int, error) {
	address := "127.0.0.1:50051"
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect %v", err)
	}
	defer conn.Close()

	joinOrderClient := NewJoinOrderClient(conn)
	//ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	//defer cancel()
	state := State{}
	state.CurrentJoinTree = encodeLogicalPlan(curJoinTree)
	for _, action := range actions {
		state.Actions = append(state.Actions, encodeLogicalPlan(action))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := joinOrderClient.GetAction(ctx, &state)
	if err != nil {
		log.Fatalf("could not greet :%v", err)
		return 0, nil
	}
	log.Printf("Greeting: %s", r.GetMessage())
	return 1, nil
}
