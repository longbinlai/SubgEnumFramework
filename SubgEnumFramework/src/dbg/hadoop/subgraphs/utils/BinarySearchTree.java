package dbg.hadoop.subgraphs.utils;

import java.util.AbstractSet;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A simple Binary Search Tree. This is NOT a balanced tree, so you probably
 * want to use RedBlackTree, which IS balanced.
 *
 * Algorithms furnished by CLRS
 */
public class BinarySearchTree< Key, Value > implements Iterable< BinarySearchTree.Node< Key, Value > > {

        public static class Node< Key, Value > {

                protected Key key;
                protected Value value;
               
                protected Node< Key, Value > left;
                protected Node< Key, Value > right;
                protected Node< Key, Value > parent;

                protected int subtreeSize;

                protected Node( Key key, Value value ) {
                        this.key = key;
                        this.value = value;
                        this.left = null;
                        this.right = null;
                        this.parent = null;
                        this.subtreeSize = 1;
                }

                /**
                 * A O(1) for building an augmented data structure.
                 * @see CLRS 14.1
                 */
                public void updateDataFromChildren() {
                        subtreeSize = 1;
                        if( left != null ) subtreeSize += left.getSubtreeSize();
                        if( right != null ) subtreeSize += right.getSubtreeSize();
                }
               
                public Key getKey() {
                        return key;
                }
               
                protected void setKey( Key key ) {
                        this.key = key;
                }
               
                public Value getValue() {
                        return value;
                }

                protected Value setValue( Value value ) {
                        Value oldValue = this.value;
                        this.value = value;
                        return oldValue;
                }
               
                @Override
                public String toString() {
                        return key + " |" + subtreeSize + "|: " + value;
                }

                protected void setParent( Node< Key, Value > parent ) {
                        this.parent = parent;
                }

                public Node< Key, Value > getParent() {
                        return parent;
                }

                protected void setLeft( Node< Key, Value > left ) {
                        this.left = left;
                }

                public Node< Key, Value > getLeft() {
                        return left;
                }

                protected void setRight( Node< Key, Value > right ) {
                        this.right = right;
                }

                public Node< Key, Value > getRight() {
                        return right;
                }

                public final boolean isRoot() {
                        return parent == null;
                }

                public final boolean isLeftChild() {
                        return parent != null && parent.left == this;
                }

                public final boolean isRightChild() {
                        return parent != null && parent.right == this;
                }

                public final boolean isLeaf() {
                        return left == null && right == null;
                }

                public final boolean hasLeftChildOnly() {
                        return left != null && right == null;
                }

                public final boolean hasRightChildOnly() {
                        return left == null && right != null;
                }

                public final boolean hasBothChildren() {
                        return left != null && right != null;
                }

                protected int getSubtreeSize() {
                        return subtreeSize;
                }

                protected void setSubtreeSize( int subtreeSize ) {
                        this.subtreeSize = subtreeSize;
                }
        }

        protected Node< Key, Value > root = null;
        protected int size = 0;
        protected Comparator< ? super Key > comparator = null;

        /**
         * Will throw {@link ClassCastException} if the keys are not {@link Comparable}
         */
        public BinarySearchTree() {
                this.comparator = null;
        }

        public BinarySearchTree( Comparator< ? super Key > comparator ) {
                this.comparator = comparator;
        }
       
        protected Node< Key, Value > getRoot() {
                return root;
        }

        protected void setRoot( Node< Key, Value > root ) {
                this.root = root;
        }

        /**
         * This factory-style construction allows easier extension from custom node
         * types.
         */
        protected Node< Key, Value > createNode( Key key, Value value ) {
                return new Node< Key, Value >( key, value );
        }
       
        /**
         * Inserts a new node into the tree.
         *
         * If the new node overwrites and old node, that old node's value is returned.
         */
        public Value insert( Key key, Value value ) {
                if ( key == null ) throw new IllegalArgumentException( "Key cannot be null" );
                Node< Key, Value > node = createNode( key, value );
                if ( getRoot() == null ) {
                        setRoot( node );
                        incrementSize();
                        updateAncestors( node );
                        fixupAfterInsert( node );
                        return node.value;
                }
                Value oldValue = insertBelowNode( node, getRoot() );
                return oldValue;
        }
        
        public Value put( Key key, Value value ){
        	return this.insert(key, value);
        }
       
        /**
         * Attempts to remove the Node from the tree.
         * Returns the previous value of the node, if one was found.
         */
        public Value delete( Key key ) {
                if ( key == null ) throw new IllegalArgumentException( "Key cannot be null" );
                final Node< Key, Value > node = getNode( key );
                if ( node == null ) return null;
                final Value oldValue = node.getValue();
                deleteNode( node );
                return oldValue;
        }

       
        /**
         * Clears the entire tree in O(1).
         */
        public void clear() {
                setRoot( null );
                size = 0;
        }
       
        public boolean containsKey( Key key ) {
                if ( key == null ) return false;
                Node< Key, Value > node = getNode( key );
                return ( node != null );
        }

        /**
         * Returns the Value from the Node with the Key value equal to the input
         */
        public Value get( Key key ) {
                if ( key == null ) throw new IllegalArgumentException( "Key cannot be null" );
                Node< Key, Value > node = getNode( key );
                if ( node == null ) return null;
                return node.value;
        }
       

        /**
         * Recursive algorithm for finding the Value at a certain index
         */
        public Value getValueAt( int index ) {
                Node< Key, Value > node = getNodeAt( index );
                if ( node == null ) return null;
                return node.getValue();
        }
       
        /**
         * Recursive algorithm for finding the Keu at a certain index
         */
        public Key getKeyAt( int index ) {
                Node< Key, Value > node = getNodeAt( index );
                if ( node == null ) return null;
                return node.getKey();
        }
       
        /**
         * Recursive algorithm for finding the Node at a certain index.
         *
         * select uses the Rank of a node, so we need to add 1 to the index
         */
        public Node< Key, Value > getNodeAt( int index ) {
                return select( getRoot(), index + 1 );
        }

        /**
         * Recursive algorithm for finding index for a node that has a given Key
         *
         * rank returns the Rank of a node, so we need to add 1 to get the index
         */
        public Integer getIndex( Key key ) {
                Node< Key, Value > node = getNode( key );
                if ( node == null ) return -1;
                return rank( node ) - 1;
        }
       
        /**
         * Returns the Node with the Key value equal to the input
         */
        public Node< Key, Value > getNode( Key key ) {
                Node< Key, Value > node = getRoot();
                while ( node != null ) {
                        int cmp = compare( key, node.getKey() );
                        if ( cmp == 0 )
                                return node;
                        else if ( cmp < 0 )
                                node = node.getLeft();
                        else
                                node = node.getRight();
                }
                return null;
        }
       
        /**
         * Returns the Node with the minimum Key value
         */
        public Node< Key, Value > getMin() {
                Node< Key, Value > node = getRoot();
                while ( node != null && node.getLeft() != null ){
                        node = node.getLeft();
                }
                return node;
        }

        /**
         * Returns the Node with the maximum Key value
         */
        public Node< Key, Value > getMax() {
                Node< Key, Value > node = getRoot();
                while ( node != null && node.getRight() != null ){
                        node = node.getRight();
                }
                return node;
        }
       
        public Node< Key, Value > getLessThanOrEqualTo( Key key ) {
                Node< Key, Value > node = getNode( key );
                if ( node != null ) return node;
                return getLessThan( key );
        }

        public Node< Key, Value > getGreaterThanOrEqualTo( Key key ) {
                Node< Key, Value > node = getNode( key );
                if ( node != null ) return node;
                return getGreaterThan( key );
        }

        /**
         * Returns the Node with the maximum Key value less than the input
         */
        public Node< Key, Value > getLessThan( Key key ) {              
                Node< Key, Value > node = getRoot();
                while ( node != null ) {
                        int cmp = compare( key, node.getKey() );
                        if ( cmp == 0 ) {
                                return getPredecessor( node );
                        } else if ( cmp < 0 ) {
                                if ( node.getLeft() == null ) return getPredecessor( node );
                                node = node.getLeft();
                        } else {
                                if ( node.getRight() == null ) return node;
                                node = node.getRight();
                        }
                }
                return null;
        }

        /**
         * Returns the Node with the minimum Key value greater than the input
         */
        public Node< Key, Value > getGreaterThan( Key key ) {
                Node< Key, Value > node = getRoot();
                while ( node != null ) {
                        int cmp = compare( key, node.getKey() );
                        if ( cmp == 0 ) {
                                return getSuccessor( node );
                        } else if ( cmp < 0 ) {
                                if ( node.getLeft() == null ) return node;
                                node = node.getLeft();
                        } else {
                                if ( node.getRight() == null ) return getSuccessor( node );
                                node = node.getRight();
                        }
                }
                return null;
        }
       
        /**
         * Returns the Node with the minimum Key value greater than the input Node's
         */
        public Node< Key, Value > getSuccessor( Node< Key, Value > node ) {
                if ( node == null )
                        return null;
                else if ( node.getRight() != null ) {
                        Node< Key, Value > parent = node.getRight();
                        while ( parent.getLeft() != null ) parent = parent.getLeft();
                        return parent;
                } else {
                        Node< Key, Value > parent = node.getParent();
                        Node< Key, Value > child = node;
                        while ( parent != null && child == parent.getRight() ) {
                                child = parent;
                                parent = parent.getParent();
                        }
                        return parent;
                }
        }

        /**
         * Returns the Node with the maximum Key value less than the input Node's
         */
        public Node< Key, Value > getPredecessor( Node< Key, Value > node ) {
                if ( node == null )
                        return null;
                else if ( node.getLeft() != null ) {
                        Node< Key, Value > parent = node.getLeft();
                        while ( parent.getRight() != null ) parent = parent.getRight();
                        return parent;
                } else {
                        Node< Key, Value > parent = node.getParent();
                        Node< Key, Value > child = node;
                        while ( parent != null && child == parent.getLeft() ) {
                                child = parent;
                                parent = parent.getParent();
                        }
                        return parent;
                }
        }
       
        /**
         * Classic BinarySearchTree insertion with iteration instead of recursion.
         * This prevents stack overflows for very large sets.
         */
        protected Value insertBelowNode( Node< Key, Value > node, Node< Key, Value > parent ) {
                while( true ){
                        int cmp = compare( node.getKey(), parent.getKey() );
       
                        // key is equal to node's
                        if ( cmp == 0 ) {
                                return parent.value;
                        }
                       
                        // key is less than node's
                        else if ( cmp < 0 ) {
                                if ( parent.getLeft() != null ) {
                                        parent = parent.getLeft();
                                        continue;
                                } else {
                                        incrementSize();
                                        parent.setLeft( node );
                                        node.setParent( parent );
                                        updateAncestors( node );
                                        fixupAfterInsert( node );
                                        return node.value;
                                }
                        }
       
                        // key is greater than or equal to node's
                        else {
                                if ( parent.getRight() != null ) {
                                        parent = parent.getRight();
                                        continue;
                                } else {
                                        incrementSize();
                                        parent.setRight( node );
                                        node.setParent( parent );
                                        updateAncestors( node );
                                        fixupAfterInsert( node );
                                        return node.value;
                                }
                        }
                }
        }
       
        /**
         * This comes mostly straight out of TreeMap. I tried to roll my own, but it
         * was violating the RedBlackTree invariants. Note that there are hooks for
         * re-balancing the tree in the RedBlackTree implementation.
         */
    protected void deleteNode( Node< Key, Value > node ) {
                decrementSize();

                // If node is strictly internal, copy successor's element to node and
                // then make node point to successor.
                if ( node.hasBothChildren() ) {
                        Node< Key, Value > successor = getSuccessor( node );
                        copyData( node, successor );
                        node = successor;
                }

                // Start fixup at replacement node, if it exists.
                final Node< Key, Value > replacement = ( node.getLeft() != null ? node.getLeft() : node.getRight() );

                if ( replacement != null ) {
                        // Link replacement to parent
                        replacement.setParent( node.getParent() );
                        if ( node.isRoot() )
                                setRoot( replacement );
                        else if ( node.isLeftChild() )
                                node.getParent().setLeft( replacement );
                        else
                                node.getParent().setRight( replacement );

                        // Null out links so they are OK to use by fixAfterDeletion.
                        detach( node );

                        // Fix replacement
                        updateAncestors( replacement );
                        fixupAfterDelete( node, replacement );
                } else if ( node.getParent() == null ) {
                        // return if we are the only node.
                        setRoot( null );
                } else {
                        // No children. Use self as phantom replacement and unlink.
                        updateAncestors( node );
                        fixupAfterDelete( node, node );
                       
                        Node<Key,Value> parent = node.getParent();
                        if ( node.isLeftChild() )
                                node.getParent().setLeft( null );
                        else if ( node.isRightChild() )
                                node.getParent().setRight( null );
                        node.setParent( null );
                       
                        // update augmentation data when we remove the last child from a node
                        updateAncestors( parent );
                }
        }

        /**
         * Straight out of CLRS
         *
         * @see CLRS 14.1
         */
        protected Node< Key, Value > select( Node< Key, Value > x, int i ) {
                if ( x == null ) return null;
                int r = sizeOf( leftOf( x ) ) + 1;
                if ( i == r )
                        return x;
                else if ( i < r )
                        return select( x.getLeft(), i );
                else
                        return select( x.getRight(), i - r );
        }

        /**
         * Straight out of CLRS
         *
         * @see CLRS 14.1
         */
        protected int rank( Node< Key, Value > x ) {
                int r = sizeOf( leftOf( x ) ) + 1;
                Node< Key, Value > y = x;
                while ( y != getRoot() ) {
                        if ( y == rightOf( parentOf( y ) ) ) {
                                r = r + sizeOf( leftOf( parentOf( y ) ) ) + 1;
                        }
                        y = parentOf( y );
                }
                return r;
        }

        // These methods make dealing with nulls at every stage a bit easier
        // taken from TreeMap
       
        @SuppressWarnings("unchecked")
        protected static < Key, Value, NodeType extends Node< Key, Value >> NodeType leftOf( NodeType p ) {
                return ( p == null ) ? null : (NodeType) p.getLeft(); /* convenience cast for extensions */
        }
       
        @SuppressWarnings("unchecked")
        protected static < Key, Value, NodeType extends Node< Key, Value >> NodeType rightOf( NodeType p ) {
                return ( p == null ) ? null : (NodeType) p.getRight(); /* convenience cast for extensions */
        }
       
        @SuppressWarnings("unchecked")
        protected static < Key, Value, NodeType extends Node< Key, Value >> NodeType parentOf( NodeType p ) {
                return ( p == null ) ? null : (NodeType) p.getParent(); /* convenience cast for extensions */
        }

        protected static < Key, Value, NodeType extends Node< Key, Value >> int sizeOf( NodeType p ) {
                return ( p == null ) ? 0 : p.getSubtreeSize(); /* convenience cast for extensions */
        }
       
        /**
         * Removes references to node's location
         */
        protected void detach( Node< Key, Value > node ) {
                node.setLeft( null );
                node.setRight( null );
                node.setParent( null );
        }
       
        /**
         * Copies the internal data from one node to another
         */
        protected void copyData( Node< Key, Value > to, Node< Key, Value > from ) {
                to.key = from.key;
                to.value = from.value;
        }
       
        @SuppressWarnings("unchecked")
        protected int compare( Key k1, Key k2 ) {
                return ( comparator == null ? ( (Comparable< Key >) k1 ).compareTo( k2 ) : comparator.compare( k1, k2 ) );
        }

        /** From CLRS */
        protected void rotateLeft( Node< Key, Value > node ) {
                Node< Key, Value > right = node.getRight();
                node.setRight( right.getLeft() );
                if ( right.getLeft() != null ) right.getLeft().setParent( node );
                right.setParent( node.getParent() );
                if ( node.getParent() == null )
                        setRoot( right );
                else if ( node.getParent().getLeft() == node )
                        node.getParent().setLeft( right );
                else
                        node.getParent().setRight( right );
                right.setLeft( node );
                node.setParent( right );
                updateAfterRotate( node, right );
        }

        /** From CLRS */
        protected void rotateRight( Node< Key, Value > node ) {
                Node< Key, Value > left = node.getLeft();
                node.setLeft( left.getRight() );
                if ( left.getRight() != null ) left.getRight().setParent( node );
                left.setParent( node.getParent() );
                if ( node.getParent() == null )
                        setRoot( left );
                else if ( node.getParent().getRight() == node )
                        node.getParent().setRight( left );
                else
                        node.getParent().setLeft( left );
                left.setRight( node );
                node.setParent( left );
                updateAfterRotate( node, left );
        }
       
        protected void fixupAfterInsert( Node< Key, Value > node ) { /* extensions should re-balance here */ }

        protected void fixupAfterDelete( Node< Key, Value > node, Node< Key, Value > x ) { /* extensions should re-balance here */ }

        /**
         * Augmentation step: walks up ancestors and updates augmentation data
         *
         * O(log(n))
         *
         * @see CLRS 14.3
         */
        protected void updateAncestors( Node< Key, Value > node ) {
                while ( node != null ) {
                        node.updateDataFromChildren();
                        node = node.getParent();
                }
        }

        /**
         * Augmentation step: updates augmentation data after rotate. It is crucial
         * to update the child first since the parent will need to update from
         * child's changes
         *
         * O(1)
         *
         * @see CLRS 14.3
         */
        protected void updateAfterRotate( Node< Key, Value > node0, Node< Key, Value > node1 ) {
                if ( node0 != null ) node0.updateDataFromChildren();
                if ( node1 != null ) node1.updateDataFromChildren();
        }

        public boolean isEmpty() {
                return ( getSize() == 0 );
        }
       
        public int getSize() {
                return this.size;
        }
       
        protected void incrementSize() {
                this.size += 1;
        }

        protected void decrementSize() {
                this.size -= 1;
        }
       
        public Comparator< ? super Key > getComparator() {
                return comparator;
        }

        public void setComparator( Comparator< ? super Key > comparator ) {
                this.comparator = comparator;
        }

        @Override
        public String toString() {
                return "[" + toString( getRoot() ) + "]";
        }
       
        protected String toString( Node< Key, Value > node ) {
                if ( node == null ) return "";
                return node.toString() + ", " + toString( node.getLeft() ) + ", " + toString( node.getRight() );
        }
       
        public Iterator< Node< Key, Value >> iterator() {
                return new NodeIterator();
        }
       
    protected class NodeIterator implements Iterator< Node< Key, Value > > {
                protected Node< Key, Value > lastReturned = null;
                protected Node< Key, Value > next;

                protected NodeIterator() {
                        next = getMin();
                }

                public boolean hasNext() {
                        return next != null;
                }

                public Node< Key, Value > next() {
                        if ( next == null ) throw new NoSuchElementException();
                        lastReturned = next;
                        next = getSuccessor( next );
                        return lastReturned;
                }

                public void remove() {
                        if ( lastReturned == null ) throw new IllegalStateException();
                        if ( lastReturned.hasBothChildren() ) next = lastReturned;
                        delete( lastReturned.getKey() );
                        lastReturned = null;
                }
        }

        protected class KeyIterator implements Iterator< Key > {
                protected final BinarySearchTree< Key, Value >.NodeIterator delegate;

                protected KeyIterator() {
                        delegate = new NodeIterator();
                }

                public boolean hasNext() {
                        return delegate.hasNext();
                }

                public Key next() {
                        Node< Key, Value > node = delegate.next();
                        return node.getKey();
                }

                public void remove() {
                        delegate.remove();
                }
        }
       
        protected class ValueIterator implements Iterator< Value > {
                protected final BinarySearchTree< Key, Value >.NodeIterator delegate;

                protected ValueIterator() {
                        delegate = new NodeIterator();
                }

                public boolean hasNext() {
                        return delegate.hasNext();
                }

                public Value next() {
                        Node< Key, Value > node = delegate.next();
                        return node.getValue();
                }

                public void remove() {
                        delegate.remove();
                }
        }
       
        public Set< Key > keySet() {
                return new AbstractSet< Key >() {
                        @Override
                        public Iterator< Key > iterator() {
                                return new KeyIterator();
                        }

                        @Override
                        public int size() {
                                return BinarySearchTree.this.getSize();
                        }

                        @Override
                        @SuppressWarnings("unchecked")
                        public boolean contains( Object o ) {
                                return containsKey( (Key) o );
                        }

                        @Override
                        @SuppressWarnings("unchecked")
                        public boolean remove( Object o ) {
                                int oldSize = size;
                                BinarySearchTree.this.delete( (Key) o );
                                return size != oldSize;
                        }

                        @Override
                        public void clear() {
                                BinarySearchTree.this.clear();
                        }
                };
        }

        public Iterable< Key > getKeys() {
                return new Iterable< Key >() {
                        public Iterator< Key > iterator() {
                                return new KeyIterator();
                        }
                };
        }

        public Iterable< Value > getValues() {
                return new Iterable< Value >() {
                        public Iterator< Value > iterator() {
                                return new ValueIterator();
                        }
                };
        }
}

