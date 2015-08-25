package dbg.hadoop.subgraphs.utils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * A RedBlackTree -- a balanced Binary Search Tree.
 *
 * This implements an ordered Map-like interface where nodes contain key-value pairs.
 *
 * Iteration occurs in increasing order of Key.
 *
 * Algorithms furnished by CLRS
 *
 * Asymptotic Characteristics:
 * Insertion -- O(log(n))
 * Deletion -- O(log(n))
 * Lookup -- O(log(n))
 */
public class RedBlackTree< Key, Value > extends BinarySearchTree< Key, Value > {

        protected static enum NodeColor {
                RED, BLACK
        }

        protected static class RedBlackNode< Key, Value > extends Node< Key, Value > {

                protected NodeColor color;

                public RedBlackNode( Key key, Value value ) {
                        super( key, value );
                        this.color = NodeColor.RED;
                }

                protected NodeColor getColor() {
                        return color;
                }

                protected void setColor( NodeColor color ) {
                        this.color = color;
                }

                /** Convenience cast */
                @Override
                public RedBlackNode< Key, Value > getLeft() {
                        return (RedBlackNode< Key, Value >) super.getLeft();
                }

                /** Convenience cast */
                @Override
                public RedBlackNode< Key, Value > getRight() {
                        return (RedBlackNode< Key, Value >) super.getRight();
                }
               
                /** Convenience cast */
                @Override
                public RedBlackNode< Key, Value > getParent() {
                        return (RedBlackNode< Key, Value >) super.getParent();
                }
        }
       
        public RedBlackTree() {
                super();
        }

        public RedBlackTree( Comparator< ? super Key > comparator ) {
                super( comparator );
        }
       
        @Override
        protected RedBlackNode< Key, Value > createNode( Key key, Value value ) {
                return new RedBlackNode< Key, Value >( key, value );
        }

        @Override
        public RedBlackNode< Key, Value > getRoot() {
                return (RedBlackNode< Key, Value >) super.getRoot();
        }

        @Override
        protected void fixupAfterInsert( Node< Key, Value > node ) {
                if ( node != null ) {
                        rebalanceAfterInsert( (RedBlackNode< Key, Value >) node );
                }
        }

        @Override
        protected void fixupAfterDelete( Node< Key, Value > node, Node< Key, Value > x ) {
                if ( node != null && ( (RedBlackNode< Key, Value >) node ).getColor() == NodeColor.BLACK ) {
                        rebalanceAfterDelete( (RedBlackNode< Key, Value >) x );
                }
        }

        /**
         * Rebalance tree after insertion
         *
         * O(log(n))
         *
         * @see CLRS 13.4 - RB-Insert-Fixup
         */
        protected void rebalanceAfterInsert( RedBlackNode< Key, Value > x ) {
                x.setColor( NodeColor.RED );
                while ( x != null && x != getRoot() && colorOf( parentOf( x ) ) == NodeColor.RED ) {
                        if ( parentOf( x ) == leftOf( parentOf( parentOf( x ) ) ) ) {
                                RedBlackNode< Key, Value > y = rightOf( parentOf( parentOf( x ) ) );
                                if ( colorOf( y ) == NodeColor.RED ) {
                                        setColor( parentOf( x ), NodeColor.BLACK );
                                        setColor( y, NodeColor.BLACK );
                                        setColor( parentOf( parentOf( x ) ), NodeColor.RED );
                                        x = parentOf( parentOf( x ) );
                                } else {
                                        if ( x == rightOf( parentOf( x ) ) ) {
                                                x = parentOf( x );
                                                rotateLeft( x );
                                        }
                                        setColor( parentOf( x ), NodeColor.BLACK );
                                        setColor( parentOf( parentOf( x ) ), NodeColor.RED );
                                        if ( parentOf( parentOf( x ) ) != null ) rotateRight( parentOf( parentOf( x ) ) );
                                }
                        } else {
                                RedBlackNode< Key, Value > y = leftOf( parentOf( parentOf( x ) ) );
                                if ( colorOf( y ) == NodeColor.RED ) {
                                        setColor( parentOf( x ), NodeColor.BLACK );
                                        setColor( y, NodeColor.BLACK );
                                        setColor( parentOf( parentOf( x ) ), NodeColor.RED );
                                        x = parentOf( parentOf( x ) );
                                } else {
                                        if ( x == leftOf( parentOf( x ) ) ) {
                                                x = parentOf( x );
                                                rotateRight( x );
                                        }
                                        setColor( parentOf( x ), NodeColor.BLACK );
                                        setColor( parentOf( parentOf( x ) ), NodeColor.RED );
                                        if ( parentOf( parentOf( x ) ) != null ) rotateLeft( parentOf( parentOf( x ) ) );
                                }
                        }
                }
                getRoot().setColor( NodeColor.BLACK );
        }

        /**
         * Rebalance tree after deletion
         *
         * O(log(n))
         * @see CLRS 13.4 - RB-Delete-Fixup
         */
        protected void rebalanceAfterDelete( RedBlackNode< Key, Value > x ) {
                while ( x != getRoot() && colorOf( x ) == NodeColor.BLACK ) {
                        if ( x == leftOf( parentOf( x ) ) ) {
                                RedBlackNode< Key, Value > sib = rightOf( parentOf( x ) );

                                if ( colorOf( sib ) == NodeColor.RED ) {
                                        setColor( sib, NodeColor.BLACK );
                                        setColor( parentOf( x ), NodeColor.RED );
                                        rotateLeft( parentOf( x ) );
                                        sib = rightOf( parentOf( x ) );
                                }

                                if ( colorOf( leftOf( sib ) ) == NodeColor.BLACK && colorOf( rightOf( sib ) ) == NodeColor.BLACK ) {
                                        setColor( sib, NodeColor.RED );
                                        x = parentOf( x );
                                } else {
                                        if ( colorOf( rightOf( sib ) ) == NodeColor.BLACK ) {
                                                setColor( leftOf( sib ), NodeColor.BLACK );
                                                setColor( sib, NodeColor.RED );
                                                rotateRight( sib );
                                                sib = rightOf( parentOf( x ) );
                                        }
                                        setColor( sib, colorOf( parentOf( x ) ) );
                                        setColor( parentOf( x ), NodeColor.BLACK );
                                        setColor( rightOf( sib ), NodeColor.BLACK );
                                        rotateLeft( parentOf( x ) );
                                        x = getRoot();
                                }
                        } else { // symmetric
                                RedBlackNode< Key, Value > sib = leftOf( parentOf( x ) );

                                if ( colorOf( sib ) == NodeColor.RED ) {
                                        setColor( sib, NodeColor.BLACK );
                                        setColor( parentOf( x ), NodeColor.RED );
                                        rotateRight( parentOf( x ) );
                                        sib = leftOf( parentOf( x ) );
                                }

                                if ( colorOf( rightOf( sib ) ) == NodeColor.BLACK && colorOf( leftOf( sib ) ) == NodeColor.BLACK ) {
                                        setColor( sib, NodeColor.RED );
                                        x = parentOf( x );
                                } else {
                                        if ( colorOf( leftOf( sib ) ) == NodeColor.BLACK ) {
                                                setColor( rightOf( sib ), NodeColor.BLACK );
                                                setColor( sib, NodeColor.RED );
                                                rotateLeft( sib );
                                                sib = leftOf( parentOf( x ) );
                                        }
                                        setColor( sib, colorOf( parentOf( x ) ) );
                                        setColor( parentOf( x ), NodeColor.BLACK );
                                        setColor( leftOf( sib ), NodeColor.BLACK );
                                        rotateRight( parentOf( x ) );
                                        x = getRoot();
                                }
                        }
                }
                setColor( x, NodeColor.BLACK );
        }
       
        // These methods make dealing with nulls at every stage a bit easier
        // taken from TreeMap
       
        protected static < Key, Value > NodeColor colorOf( RedBlackNode< Key, Value > p ) {
                return ( p == null ? NodeColor.BLACK : p.getColor() );
        }

        protected static < Key, Value > void setColor( RedBlackNode< Key, Value > p, NodeColor c ) {
                if ( p != null ) p.setColor( c );
        }
        
        public static void main(String[] args){
        	/*
        	RedBlackTree<Integer, ArrayList<Integer>> map = new RedBlackTree<Integer, ArrayList<Integer>>(); 
        	Node<Integer, ArrayList<Integer>> key;
    		for(int i = 0; i < 1000000; ++i){
    			//map.insert(i, 0);
    			if((key = map.getNode(i)) == null){
    				ArrayList<Integer> list = map.insert(i, new ArrayList());
    				for(int j = 0; j < 500; ++j){
    					list.add(j);
    				}
    			}
    			else{
    				for(int j = 0; j < 500; ++j){
    					key.getValue().add(j);
    				}
    			}
    			
    			
    		}*/
        	Map<Integer, ArrayList<Integer>> map = new HashMap<Integer, ArrayList<Integer>>();
        	for(int i = 0; i < 1000000; ++i){
        		map.put(i, new ArrayList<Integer>());
        		for(int j = 0; j < 500; ++j){
        			map.get(i).add(j);
        		}
        	}
    		Utility.printMemoryUsage();
        }
}

