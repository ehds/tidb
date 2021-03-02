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
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
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
		var err error
		var newNode *jrNode
		if s.use_dqn_server() {
			fmt.Println("use dqn")
			newNode, err = s.constructConnectedJoinTreeDQN()
		} else {
			fmt.Println("use greedy")
			newNode, err = s.constructConnectedJoinTree()
		}

		if err != nil {
			return nil, err
		}
		cartesianGroup = append(cartesianGroup, newNode.p)
	}

	return s.makeBushyJoin(cartesianGroup), nil
}

func (s *joinReorderGreedySolver) use_dqn_server() bool {
	address := "127.0.0.1:50051"
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect %v", err)
	}
	defer conn.Close()

	joinOrderClient := NewJoinOrderClient(conn)
	empty := Empty{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := joinOrderClient.IsDQN(ctx, &empty)
	if err != nil {
		return false
	}
	return r.Value
}
func (s *joinReorderGreedySolver) constructConnectedJoinTree() (*jrNode, error) {
	curJoinTree := s.curJoinGroup[0]
	s.curJoinGroup = s.curJoinGroup[1:]
	for {
		bestCost := math.MaxFloat64
		bestIdx := -1
		var finalRemainOthers []expression.Expression
		var bestJoin LogicalPlan
		for i, node := range s.curJoinGroup {
			newJoin, remainOthers := s.checkConnectionAndMakeJoin(curJoinTree.p, node.p)
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

func (s *joinReorderGreedySolver) constructConnectedJoinTreeDQN() (*jrNode, error) {
	curJoinTree := s.curJoinGroup[0]
	s.curJoinGroup = s.curJoinGroup[1:]
	for {
		bestCost := math.MaxFloat64
		bestIdx := -1
		var finalRemainOthers []expression.Expression
		var bestJoin LogicalPlan
		var testActions []*jrNode
		var originIndex []int
		for i, node := range s.curJoinGroup {
			newJoin, _ := s.checkConnectionAndMakeJoin(curJoinTree.p, node.p)
			if newJoin == nil {
				continue
			}
			testActions = append(testActions, node)
			originIndex = append(originIndex, i)
		}
		if len(testActions) == 0 {
			break
		}
		bestActionIndex, err := s.getAction(curJoinTree.p, testActions)
		if err != nil {
			break
		}
		bestAction := testActions[bestActionIndex]
		bestJoin, finalRemainOthers = s.checkConnectionAndMakeJoin(curJoinTree.p, bestAction.p)
		_, err = bestJoin.recursiveDeriveStats(nil)
		bestCost = s.calcJoinCumCost(bestJoin, curJoinTree, bestAction)
		bestIdx = originIndex[bestActionIndex]
		//curCost := s.calcJoinCumCost(bestJoin, curJoinTree, bestAction)
		// for i, node := range s.curJoinGroup {
		// 	newJoin, remainOthers := s.checkConnectionAndMakeJoin(curJoinTree.p, node.p)
		// 	testActions = append(testActions, node.p)
		// 	if newJoin == nil {
		// 		continue
		// 	}
		// 	_, err := newJoin.recursiveDeriveStats(nil)
		// 	if err != nil {
		// 		return nil, err
		// 	}
		// 	curCost := s.calcJoinCumCost(newJoin, curJoinTree, node)
		// 	if bestCost > curCost {
		// 		bestCost = curCost
		// 		bestJoin = newJoin
		// 		bestIdx = i
		// 		finalRemainOthers = remainOthers
		// 	}
		// }
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

func encodeLogicalPlan(p LogicalPlan) *LogicalNode {
	var currentNode = LogicalNode{}
	switch x := p.(type) {
	case *LogicalJoin:
		var joinNode = LogicalJoinNode{Tp: x.TP(), JoinType: x.JoinType.String()}
		for _, child := range p.Children() {
			currentNode.Childrens = append(currentNode.Childrens, encodeLogicalPlan(child))
		}
		for _, condition := range x.EqualConditions {
			var args []string
			funcname := condition.FuncName.String()
			for _, arg := range condition.GetArgs() {
				args = append(args, arg.String())
			}
			joinNode.Conditions = append(joinNode.Conditions, &Condition{Funcname: funcname, Args: args})
		}
		currentNode.Node = &LogicalNode_JoinNode{&joinNode}

	case *DataSource:
		var tableNode = LogicalTableNode{Tp: x.TP(), TableName: x.TableInfo().Name.L}
		for _, condition := range x.allConds {
			if scalarFunc, isScalar := condition.(*expression.ScalarFunction); isScalar {
				var args []string
				funcname := scalarFunc.FuncName.String()
				for _, arg := range scalarFunc.GetArgs() {
					args = append(args, arg.String())
				}
				tableNode.Conditions = append(tableNode.Conditions, &Condition{Funcname: funcname, Args: args})
			}
		}
		currentNode.Node = &LogicalNode_TableNode{&tableNode}
	}
	return &currentNode
}
func (s *joinReorderGreedySolver) getAction(curJoinTree LogicalPlan, actions []*jrNode) (int, error) {
	fmt.Println(len(actions))
	if len(actions) == 1 {
		fmt.Println("not need to connect")
		return 0, nil
	}
	originalSQL := s.ctx.GetSessionVars().StmtCtx.OriginalSQL
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
	state.OriginalSql = originalSQL
	state.CurrentJoinTree = encodeLogicalPlan(curJoinTree)
	for _, action := range actions {
		state.Actions = append(state.Actions, encodeLogicalPlan(action.p))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := joinOrderClient.GetAction(ctx, &state)
	if err != nil {
		log.Fatalf("could not greet :%v", err)
		return 0, nil
	}
	log.Printf("Greeting: %s", r.GetMessage())
	index, err := strconv.Atoi(r.GetMessage())
	if err != nil {
		return 0, err
	}
	return index, nil
}
