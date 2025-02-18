use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use crate::Result;

macro_rules! handle_transform_recursion {
    ($F_DOWN:expr, $F_CHILD:expr, $F_UP:expr) => {{
        $F_DOWN?
            .transform_children(|n| n.map_children($F_CHILD))?
            .transform_parent($F_UP)
    }};
}

pub trait TreeNode: Sized {
    /// Visit the tree node with a [`TreeNodeVisitor`], performing a
    /// depth-first walk of the node and its children.
    ///
    /// [`TreeNodeVisitor::f_down()`] is called in top-down order (before
    /// children are visited), [`TreeNodeVisitor::f_up()`] is called in
    /// bottom-up order (after children are visited).
    ///
    /// # Return Value
    /// Specifies how the tree walk ended. See [`TreeNodeRecursion`] for details.
    ///
    /// # See Also:
    /// * [`Self::apply`] for inspecting nodes with a closure
    /// * [`Self::rewrite`] to rewrite owned `TreeNode`s
    ///
    /// # Example
    /// Consider the following tree structure:
    /// ```text
    /// ParentNode
    ///    left: ChildNode1
    ///    right: ChildNode2
    /// ```
    ///
    /// Here, the nodes would be visited using the following order:
    /// ```text
    /// TreeNodeVisitor::f_down(ParentNode)
    /// TreeNodeVisitor::f_down(ChildNode1)
    /// TreeNodeVisitor::f_up(ChildNode1)
    /// TreeNodeVisitor::f_down(ChildNode2)
    /// TreeNodeVisitor::f_up(ChildNode2)
    /// TreeNodeVisitor::f_up(ParentNode)
    /// ```
    fn visit<'n, V: TreeNodeVisitor<'n, Node = Self>>(
        &'n self,
        visitor: &mut V,
    ) -> Result<TreeNodeRecursion> {
        visitor
            .f_down(self)?
            .visit_children(|| self.apply_children(|c| c.visit(visitor)))?
            .visit_parent(|| visitor.f_up(self))
    }

    /// Rewrite the tree node with a [`TreeNodeRewriter`], performing a
    /// depth-first walk of the node and its children.
    ///
    /// [`TreeNodeRewriter::f_down()`] is called in top-down order (before
    /// children are visited), [`TreeNodeRewriter::f_up()`] is called in
    /// bottom-up order (after children are visited).
    ///
    /// Note: If using the default [`TreeNodeRewriter::f_up`] or
    /// [`TreeNodeRewriter::f_down`] that do nothing, consider using
    /// [`Self::transform_down`] instead.
    ///
    /// # Return Value
    /// The returns value specifies how the tree walk should proceed. See
    /// [`TreeNodeRecursion`] for details. If an [`Err`] is returned, the
    /// recursion stops immediately.
    ///
    /// # See Also
    /// * [`Self::visit`] for inspecting (without modification) `TreeNode`s
    /// * [Self::transform_down_up] for a top-down (pre-order) traversal.
    /// * [Self::transform_down] for a top-down (pre-order) traversal.
    /// * [`Self::transform_up`] for a bottom-up (post-order) traversal.
    ///
    /// # Example
    /// Consider the following tree structure:
    /// ```text
    /// ParentNode
    ///    left: ChildNode1
    ///    right: ChildNode2
    /// ```
    ///
    /// Here, the nodes would be visited using the following order:
    /// ```text
    /// TreeNodeRewriter::f_down(ParentNode)
    /// TreeNodeRewriter::f_down(ChildNode1)
    /// TreeNodeRewriter::f_up(ChildNode1)
    /// TreeNodeRewriter::f_down(ChildNode2)
    /// TreeNodeRewriter::f_up(ChildNode2)
    /// TreeNodeRewriter::f_up(ParentNode)
    /// ```
    fn rewrite<R: TreeNodeRewriter<Node = Self>>(
        self,
        rewriter: &mut R,
    ) -> Result<Transformed<Self>> {
        handle_transform_recursion!(rewriter.f_down(self), |c| c.rewrite(rewriter), |n| {
            rewriter.f_up(n)
        })
    }

    /// Applies `f` to the node then each of its children, recursively (a
    /// top-down, pre-order traversal).
    ///
    /// The return [`TreeNodeRecursion`] controls the recursion and can cause
    /// an early return.
    ///
    /// # See Also
    /// * [`Self::transform_down`] for the equivalent transformation API.
    /// * [`Self::visit`] for both top-down and bottom up traversal.
    fn apply<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        fn apply_impl<'n, N: TreeNode, F: FnMut(&'n N) -> Result<TreeNodeRecursion>>(
            node: &'n N,
            f: &mut F,
        ) -> Result<TreeNodeRecursion> {
            f(node)?.visit_children(|| node.apply_children(|c| apply_impl(c, f)))
        }

        apply_impl(self, &mut f)
    }

    /// Recursively rewrite the node's children and then the node using `f`
    /// (a bottom-up post-order traversal).
    ///
    /// A synonym of [`Self::transform_up`].
    fn transform<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        self.transform_up(f)
    }

    /// Recursively rewrite the tree using `f` in a top-down (pre-order)
    /// fashion.
    ///
    /// `f` is applied to the node first, and then its children.
    ///
    /// # See Also
    /// * [`Self::transform_up`] for a bottom-up (post-order) traversal.
    /// * [Self::transform_down_up] for a combined traversal with closures
    /// * [`Self::rewrite`] for a combined traversal with a visitor
    fn transform_down<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        fn transform_down_impl<N: TreeNode, F: FnMut(N) -> Result<Transformed<N>>>(
            node: N,
            f: &mut F,
        ) -> Result<Transformed<N>> {
            f(node)?.transform_children(|n| n.map_children(|c| transform_down_impl(c, f)))
        }

        transform_down_impl(self, &mut f)
    }

    /// Recursively rewrite the node using `f` in a bottom-up (post-order)
    /// fashion.
    ///
    /// `f` is applied to the node's  children first, and then to the node itself.
    ///
    /// # See Also
    /// * [`Self::transform_down`] top-down (pre-order) traversal.
    /// * [Self::transform_down_up] for a combined traversal with closures
    /// * [`Self::rewrite`] for a combined traversal with a visitor
    fn transform_up<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        fn transform_up_impl<N: TreeNode, F: FnMut(N) -> Result<Transformed<N>>>(
            node: N,
            f: &mut F,
        ) -> Result<Transformed<N>> {
            node.map_children(|c| transform_up_impl(c, f))?
                .transform_parent(f)
        }

        transform_up_impl(self, &mut f)
    }

    /// Transforms the node using `f_down` while traversing the tree top-down
    /// (pre-order), and using `f_up` while traversing the tree bottom-up
    /// (post-order).
    ///
    /// The method behaves the same as calling [`Self::transform_down`] followed
    /// by [`Self::transform_up`] on the same node. Use this method if you want
    /// to start the `f_up` process right where `f_down` jumps. This can make
    /// the whole process faster by reducing the number of `f_up` steps.
    ///
    /// # See Also
    /// * [`Self::transform_up`] for a bottom-up (post-order) traversal.
    /// * [Self::transform_down] for a top-down (pre-order) traversal.
    /// * [`Self::rewrite`] for a combined traversal with a visitor
    ///
    /// # Example
    /// Consider the following tree structure:
    /// ```text
    /// ParentNode
    ///    left: ChildNode1
    ///    right: ChildNode2
    /// ```
    ///
    /// The nodes are visited using the following order:
    /// ```text
    /// f_down(ParentNode)
    /// f_down(ChildNode1)
    /// f_up(ChildNode1)
    /// f_down(ChildNode2)
    /// f_up(ChildNode2)
    /// f_up(ParentNode)
    /// ```
    ///
    /// See [`TreeNodeRecursion`] for more details on controlling the traversal.
    ///
    /// If `f_down` or `f_up` returns [`Err`], the recursion stops immediately.
    ///
    /// Example:
    /// ```text
    ///                                               |   +---+
    ///                                               |   | J |
    ///                                               |   +---+
    ///                                               |     |
    ///                                               |   +---+
    ///                  TreeNodeRecursion::Continue  |   | I |
    ///                                               |   +---+
    ///                                               |     |
    ///                                               |   +---+
    ///                                              \|/  | F |
    ///                                               '   +---+
    ///                                                  /     \ ___________________
    ///                  When `f_down` is           +---+                           \ ---+
    ///                  applied on node "E",       | E |                            | G |
    ///                  it returns with "Jump".    +---+                            +---+
    ///                                               |                                |
    ///                                             +---+                            +---+
    ///                                             | C |                            | H |
    ///                                             +---+                            +---+
    ///                                             /   \
    ///                                        +---+     +---+
    ///                                        | B |     | D |
    ///                                        +---+     +---+
    ///                                                    |
    ///                                                  +---+
    ///                                                  | A |
    ///                                                  +---+
    ///
    /// Instead of starting from leaf nodes, `f_up` starts from the node "E".
    ///                                                   +---+
    ///                                               |   | J |
    ///                                               |   +---+
    ///                                               |     |
    ///                                               |   +---+
    ///                                               |   | I |
    ///                                               |   +---+
    ///                                               |     |
    ///                                              /    +---+
    ///                                            /      | F |
    ///                                          /        +---+
    ///                                        /         /     \ ______________________
    ///                                       |     +---+   .                          \ ---+
    ///                                       |     | E |  /|\  After `f_down` jumps    | G |
    ///                                       |     +---+   |   on node E, `f_up`       +---+
    ///                                        \------| ---/   if applied on node E.      |
    ///                                             +---+                               +---+
    ///                                             | C |                               | H |
    ///                                             +---+                               +---+
    ///                                             /   \
    ///                                        +---+     +---+
    ///                                        | B |     | D |
    ///                                        +---+     +---+
    ///                                                    |
    ///                                                  +---+
    ///                                                  | A |
    ///                                                  +---+
    /// ```
    fn transform_down_up<
        FD: FnMut(Self) -> Result<Transformed<Self>>,
        FU: FnMut(Self) -> Result<Transformed<Self>>,
    >(
        self,
        mut f_down: FD,
        mut f_up: FU,
    ) -> Result<Transformed<Self>> {
        fn transform_down_up_impl<
            N: TreeNode,
            FD: FnMut(N) -> Result<Transformed<N>>,
            FU: FnMut(N) -> Result<Transformed<N>>,
        >(
            node: N,
            f_down: &mut FD,
            f_up: &mut FU,
        ) -> Result<Transformed<N>> {
            handle_transform_recursion!(
                f_down(node),
                |c| transform_down_up_impl(c, f_down, f_up),
                f_up
            )
        }

        transform_down_up_impl(self, &mut f_down, &mut f_up)
    }

    /// Returns true if `f` returns true for any node in the tree.
    ///
    /// Stops recursion as soon as a matching node is found
    fn exists<F: FnMut(&Self) -> Result<bool>>(&self, mut f: F) -> Result<bool> {
        let mut found = false;
        self.apply(|n| {
            Ok(if f(n)? {
                found = true;
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })
            .map(|_| found)
    }

    /// Low-level API used to implement other APIs.
    ///
    /// If you want to implement the [`TreeNode`] trait for your own type, you
    /// should implement this method and [`Self::map_children`].
    ///
    /// Users should use one of the higher level APIs described on [`Self`].
    ///
    /// Description: Apply `f` to inspect node's children (but not the node
    /// itself).
    fn apply_children<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        f: F,
    ) -> Result<TreeNodeRecursion>;

    /// Low-level API used to implement other APIs.
    ///
    /// If you want to implement the [`TreeNode`] trait for your own type, you
    /// should implement this method and [`Self::apply_children`].
    ///
    /// Users should use one of the higher level APIs described on [`Self`].
    ///
    /// Description: Apply `f` to rewrite the node's children (but not the node itself).
    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>>;
}

pub trait TreeNodeVisitor<'n>: Sized {
    /// The node type which is visitable.
    type Node: TreeNode;

    /// Invoked while traversing down the tree, before any children are visited.
    /// Default implementation continues the recursion.
    fn f_down(&mut self, _node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    /// Invoked while traversing up the tree after children are visited. Default
    /// implementation continues the recursion.
    fn f_up(&mut self, _node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
}

pub trait TreeNodeRewriter: Sized {
    /// The node type which is rewritable.
    type Node: TreeNode;

    /// Invoked while traversing down the tree before any children are rewritten.
    /// Default implementation returns the node as is and continues recursion.
    fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    /// Invoked while traversing up the tree after all children have been rewritten.
    /// Default implementation returns the node as is and continues recursion.
    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }
}

/// Controls how [`TreeNode`] recursions should proceed.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TreeNodeRecursion {
    /// Continue recursion with the next node.
    Continue,
    /// In top-down traversals, skip recursing into children but continue with
    /// the next node, which actually means pruning of the subtree.
    ///
    /// In bottom-up traversals, bypass calling bottom-up closures till the next
    /// leaf node.
    ///
    /// In combined traversals, if it is the `f_down` (pre-order) phase, execution
    /// "jumps" to the next `f_up` (post-order) phase by shortcutting its children.
    /// If it is the `f_up` (post-order) phase, execution "jumps" to the next `f_down`
    /// (pre-order) phase by shortcutting its parent nodes until the first parent node
    /// having unvisited children path.
    Jump,
    /// Stop recursion.
    Stop,
}

impl TreeNodeRecursion {
    /// Continues visiting nodes with `f` depending on the current [`TreeNodeRecursion`]
    /// value and the fact that `f` is visiting the current node's children.
    pub fn visit_children<F: FnOnce() -> Result<TreeNodeRecursion>>(
        self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        match self {
            TreeNodeRecursion::Continue => f(),
            TreeNodeRecursion::Jump => Ok(TreeNodeRecursion::Continue),
            TreeNodeRecursion::Stop => Ok(self),
        }
    }

    /// Continues visiting nodes with `f` depending on the current [`TreeNodeRecursion`]
    /// value and the fact that `f` is visiting the current node's sibling.
    pub fn visit_sibling<F: FnOnce() -> Result<TreeNodeRecursion>>(
        self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        match self {
            TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => f(),
            TreeNodeRecursion::Stop => Ok(self),
        }
    }

    /// Continues visiting nodes with `f` depending on the current [`TreeNodeRecursion`]
    /// value and the fact that `f` is visiting the current node's parent.
    pub fn visit_parent<F: FnOnce() -> Result<TreeNodeRecursion>>(
        self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        match self {
            TreeNodeRecursion::Continue => f(),
            TreeNodeRecursion::Jump | TreeNodeRecursion::Stop => Ok(self),
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct Transformed<T> {
    pub data: T,
    pub transformed: bool,
    pub tnr: TreeNodeRecursion,
}

impl<T> Transformed<T> {
    /// Create a new `Transformed` object with the given information.
    pub fn new(data: T, transformed: bool, tnr: TreeNodeRecursion) -> Self {
        Self {
            data,
            transformed,
            tnr,
        }
    }

    /// Create a `Transformed` with `transformed` and [`TreeNodeRecursion::Continue`].
    pub fn new_transformed(data: T, transformed: bool) -> Self {
        Self::new(data, transformed, TreeNodeRecursion::Continue)
    }

    /// Wrapper for transformed data with [`TreeNodeRecursion::Continue`] statement.
    pub fn yes(data: T) -> Self {
        Self::new(data, true, TreeNodeRecursion::Continue)
    }

    /// Wrapper for unchanged data with [`TreeNodeRecursion::Continue`] statement.
    pub fn no(data: T) -> Self {
        Self::new(data, false, TreeNodeRecursion::Continue)
    }

    /// Applies an infallible `f` to the data of this [`Transformed`] object,
    /// without modifying the `transformed` flag.
    pub fn update_data<U, F: FnOnce(T) -> U>(self, f: F) -> Transformed<U> {
        Transformed::new(f(self.data), self.transformed, self.tnr)
    }

    /// Applies a fallible `f` (returns `Result`) to the data of this
    /// [`Transformed`] object, without modifying the `transformed` flag.
    pub fn map_data<U, F: FnOnce(T) -> Result<U>>(self, f: F) -> Result<Transformed<U>> {
        f(self.data).map(|data| Transformed::new(data, self.transformed, self.tnr))
    }

    /// Applies a fallible transforming `f` to the data of this [`Transformed`]
    /// object.
    ///
    /// The returned `Transformed` object has the `transformed` flag set if either
    /// `self` or the return value of `f` have the `transformed` flag set.
    pub fn transform_data<U, F: FnOnce(T) -> Result<Transformed<U>>>(
        self,
        f: F,
    ) -> Result<Transformed<U>> {
        f(self.data).map(|mut t| {
            t.transformed |= self.transformed;
            t
        })
    }

    /// Maps the [`Transformed`] object to the result of the given `f` depending on the
    /// current [`TreeNodeRecursion`] value and the fact that `f` is changing the current
    /// node's children.
    pub fn transform_children<F: FnOnce(T) -> Result<Transformed<T>>>(
        mut self,
        f: F,
    ) -> Result<Transformed<T>> {
        match self.tnr {
            TreeNodeRecursion::Continue => {
                return f(self.data).map(|mut t| {
                    t.transformed |= self.transformed;
                    t
                });
            }
            TreeNodeRecursion::Jump => {
                self.tnr = TreeNodeRecursion::Continue;
            }
            TreeNodeRecursion::Stop => {}
        }
        Ok(self)
    }

    /// Maps the [`Transformed`] object to the result of the given `f` depending on the
    /// current [`TreeNodeRecursion`] value and the fact that `f` is changing the current
    /// node's sibling.
    pub fn transform_sibling<F: FnOnce(T) -> Result<Transformed<T>>>(
        self,
        f: F,
    ) -> Result<Transformed<T>> {
        match self.tnr {
            TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {
                f(self.data).map(|mut t| {
                    t.transformed |= self.transformed;
                    t
                })
            }
            TreeNodeRecursion::Stop => Ok(self),
        }
    }

    /// Maps the [`Transformed`] object to the result of the given `f` depending on the
    /// current [`TreeNodeRecursion`] value and the fact that `f` is changing the current
    /// node's parent.
    pub fn transform_parent<F: FnOnce(T) -> Result<Transformed<T>>>(
        self,
        f: F,
    ) -> Result<Transformed<T>> {
        match self.tnr {
            TreeNodeRecursion::Continue => f(self.data).map(|mut t| {
                t.transformed |= self.transformed;
                t
            }),
            TreeNodeRecursion::Jump | TreeNodeRecursion::Stop => Ok(self),
        }
    }
}

/// [`TreeNodeContainer`] contains elements that a function can be applied on or mapped.
/// The elements of the container are siblings so the continuation rules are similar to
/// [`TreeNodeRecursion::visit_sibling`] / [`Transformed::transform_sibling`].
pub trait TreeNodeContainer<'a, T: 'a>: Sized {
    /// Applies `f` to all elements of the container.
    /// This method is usually called from [`TreeNode::apply_children`] implementations as
    /// a node is actually a container of the node's children.
    fn apply_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &'a self,
        f: F,
    ) -> Result<TreeNodeRecursion>;

    /// Maps all elements of the container with `f`.
    /// This method is usually called from [`TreeNode::map_children`] implementations as
    /// a node is actually a container of the node's children.
    fn map_elements<F: FnMut(T) -> Result<Transformed<T>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>>;
}

impl<'a, T: 'a, C: TreeNodeContainer<'a, T>> TreeNodeContainer<'a, T> for Box<C> {
    fn apply_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &'a self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        self.as_ref().apply_elements(f)
    }

    fn map_elements<F: FnMut(T) -> Result<Transformed<T>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        (*self).map_elements(f)?.map_data(|c| Ok(Self::new(c)))
    }
}

impl<'a, T: 'a, C: TreeNodeContainer<'a, T> + Clone> TreeNodeContainer<'a, T> for Arc<C> {
    fn apply_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &'a self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        self.as_ref().apply_elements(f)
    }

    fn map_elements<F: FnMut(T) -> Result<Transformed<T>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        Arc::unwrap_or_clone(self)
            .map_elements(f)?
            .map_data(|c| Ok(Arc::new(c)))
    }
}

impl<'a, T: 'a, C: TreeNodeContainer<'a, T>> TreeNodeContainer<'a, T> for Option<C> {
    fn apply_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &'a self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        match self {
            Some(t) => t.apply_elements(f),
            None => Ok(TreeNodeRecursion::Continue),
        }
    }

    fn map_elements<F: FnMut(T) -> Result<Transformed<T>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        self.map_or(Ok(Transformed::no(None)), |c| {
            c.map_elements(f)?.map_data(|c| Ok(Some(c)))
        })
    }
}

impl<'a, T: 'a, C: TreeNodeContainer<'a, T>> TreeNodeContainer<'a, T> for Vec<C> {
    fn apply_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &'a self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        let mut tnr = TreeNodeRecursion::Continue;
        for c in self {
            tnr = c.apply_elements(&mut f)?;
            match tnr {
                TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {}
                TreeNodeRecursion::Stop => return Ok(TreeNodeRecursion::Stop),
            }
        }
        Ok(tnr)
    }

    fn map_elements<F: FnMut(T) -> Result<Transformed<T>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        let mut tnr = TreeNodeRecursion::Continue;
        let mut transformed = false;
        self.into_iter()
            .map(|c| match tnr {
                TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {
                    c.map_elements(&mut f).map(|result| {
                        tnr = result.tnr;
                        transformed |= result.transformed;
                        result.data
                    })
                }
                TreeNodeRecursion::Stop => Ok(c),
            })
            .collect::<Result<Vec<_>>>()
            .map(|data| Transformed::new(data, transformed, tnr))
    }
}

impl<'a, T: 'a, K: Eq + Hash, C: TreeNodeContainer<'a, T>> TreeNodeContainer<'a, T>
for HashMap<K, C>
{
    fn apply_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &'a self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        let mut tnr = TreeNodeRecursion::Continue;
        for c in self.values() {
            tnr = c.apply_elements(&mut f)?;
            match tnr {
                TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {}
                TreeNodeRecursion::Stop => return Ok(TreeNodeRecursion::Stop),
            }
        }
        Ok(tnr)
    }

    fn map_elements<F: FnMut(T) -> Result<Transformed<T>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        let mut tnr = TreeNodeRecursion::Continue;
        let mut transformed = false;
        self.into_iter()
            .map(|(k, c)| match tnr {
                TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {
                    c.map_elements(&mut f).map(|result| {
                        tnr = result.tnr;
                        transformed |= result.transformed;
                        (k, result.data)
                    })
                }
                TreeNodeRecursion::Stop => Ok((k, c)),
            })
            .collect::<Result<HashMap<_, _>>>()
            .map(|data| Transformed::new(data, transformed, tnr))
    }
}

impl<'a, T: 'a, C0: TreeNodeContainer<'a, T>, C1: TreeNodeContainer<'a, T>>
TreeNodeContainer<'a, T> for (C0, C1)
{
    fn apply_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &'a self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        self.0
            .apply_elements(&mut f)?
            .visit_sibling(|| self.1.apply_elements(&mut f))
    }

    fn map_elements<F: FnMut(T) -> Result<Transformed<T>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        self.0
            .map_elements(&mut f)?
            .map_data(|new_c0| Ok((new_c0, self.1)))?
            .transform_sibling(|(new_c0, c1)| {
                c1.map_elements(&mut f)?
                    .map_data(|new_c1| Ok((new_c0, new_c1)))
            })
    }
}

impl<
    'a,
    T: 'a,
    C0: TreeNodeContainer<'a, T>,
    C1: TreeNodeContainer<'a, T>,
    C2: TreeNodeContainer<'a, T>,
> TreeNodeContainer<'a, T> for (C0, C1, C2)
{
    fn apply_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &'a self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        self.0
            .apply_elements(&mut f)?
            .visit_sibling(|| self.1.apply_elements(&mut f))?
            .visit_sibling(|| self.2.apply_elements(&mut f))
    }

    fn map_elements<F: FnMut(T) -> Result<Transformed<T>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        self.0
            .map_elements(&mut f)?
            .map_data(|new_c0| Ok((new_c0, self.1, self.2)))?
            .transform_sibling(|(new_c0, c1, c2)| {
                c1.map_elements(&mut f)?
                    .map_data(|new_c1| Ok((new_c0, new_c1, c2)))
            })?
            .transform_sibling(|(new_c0, new_c1, c2)| {
                c2.map_elements(&mut f)?
                    .map_data(|new_c2| Ok((new_c0, new_c1, new_c2)))
            })
    }
}