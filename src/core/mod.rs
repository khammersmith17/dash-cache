use std::cell::{Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::hash::Hash;
use std::rc::{Rc, Weak};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("Key does not exist in cache")]
    KeyNotExist,
}
#[derive(Debug)]
struct ListNode<K, T> {
    value: T,
    key: K,
    prev: Option<Weak<RefCell<ListNode<K, T>>>>,
    next: Option<Weak<RefCell<ListNode<K, T>>>>,
}

/// LruCache is desgined for single threaded access or to be used in a non async context.
/// Initialize with capacity is required.
/// When the cache is full, the least recently used item will be evicted.
/// A use is defined as a write to the cache or a read from the cache.
#[derive(Debug)]
pub struct LruCache<K, T> {
    cap: usize,
    node_map: HashMap<K, Rc<RefCell<ListNode<K, T>>>>,
    head: Option<Weak<RefCell<ListNode<K, T>>>>,
    tail: Option<Weak<RefCell<ListNode<K, T>>>>,
}

impl<K, T> LruCache<K, T>
where
    K: Hash + Eq + Clone,
    T: Clone,
{
    /// Only provided constructor
    /// Will initialize an LruCache with the requested capacity
    pub fn with_capacity(cap: usize) -> LruCache<K, T> {
        let node_map: HashMap<K, Rc<RefCell<ListNode<K, T>>>> = HashMap::with_capacity(cap);
        LruCache {
            cap,
            node_map,
            head: None,
            tail: None,
        }
    }

    /// Returns whether or not a key exists in the cache.
    /// This method is not defined as a use, thus accessing this key will not promote the item.
    pub fn contains(&self, key: &K) -> bool {
        self.node_map.contains_key(key)
    }

    /// Update an item that exists in the cache.
    /// If the requested key does not exist in the cache, a CacheError will be returned.
    /// On success, a unit type value is returned.
    /// When the value of a key value pair is updated, this key value pair is promoted to most
    /// recently used. There is no get_mut method on this type due to borrowing semantic
    /// limitations, use this method any time you would like to mutate the value stored with a
    /// given key.
    pub fn update(&mut self, key: &K, value: T) -> Result<(), CacheError> {
        #[cfg(debug_assertions)]
        {
            self.assert_invariants();
        }
        let node_rc = {
            let Some(rc) = self.node_map.get(&key) else {
                return Err(CacheError::KeyNotExist);
            };
            rc.clone()
        };

        if let Some(head_clone) = self.head.clone() {
            if let Some(head_rc) = head_clone.upgrade() {
                if !Rc::ptr_eq(&head_rc, &node_rc) {
                    self.unlink_node(Rc::clone(&node_rc));
                    let node_weak_ref = Rc::downgrade(&node_rc);
                    self.push_node_to_head(node_weak_ref);
                }
            }
        }
        let mut node_ref: RefMut<ListNode<K, T>> = node_rc.as_ref().borrow_mut();
        node_ref.value = value;
        Ok(())
    }

    // empty is defined as both head and tail are None and the internal node_map is empty
    fn is_empty(&self) -> bool {
        self.head.is_none() && self.tail.is_none() && self.node_map.len() == 0
    }

    fn is_full(&self) -> bool {
        self.node_map.len() == self.cap
    }

    /// Insert value into the cache.
    /// If the key currently exists in the cache, the value is updated
    /// Key value pair is promoted to most recently used.
    pub fn insert(&mut self, key: K, value: T) {
        self.assert_invariants();
        match self.node_map.contains_key(&key) {
            true => {
                let _ = self.update(&key, value);
            }
            false => {
                let new_node = Rc::new(RefCell::new(ListNode {
                    key: key.clone(),
                    value,
                    prev: None,
                    next: None,
                }));

                if self.is_full() {
                    self.pop_tail()
                }

                self.push_node_to_head(Rc::downgrade(&new_node));
                self.node_map.insert(key, new_node);
            }
        }
    }

    /// Empty the cache.
    /// After this call, cache will be empty.
    pub fn drain(&mut self) {
        self.head = None;
        self.tail = None;
        self.node_map.clear();
    }

    #[cfg(debug_assertions)]
    fn assert_invariants(&self) {
        debug_assert!(
            (self.head.is_some() && self.tail.is_some())
                || (self.head.is_none() && self.tail.is_none())
        )
    }

    fn unlink_node(&mut self, node: Rc<RefCell<ListNode<K, T>>>) {
        self.assert_invariants();
        // if the list is empty, then no movement needs to happen
        if self.is_empty() {
            return;
        }

        // get weak reference to prev and next
        // pull mutable reference from RefCell
        // mutable reference to enfore poth list pointers are null after unlinking
        let mut node_ref = node.borrow_mut();
        let prev_weak = node_ref.prev.clone();
        let next_weak = node_ref.next.clone();

        // set prev next to node next
        if let Some(ref prev) = prev_weak {
            if let Some(prev_rc) = prev.upgrade() {
                let mut prev_ref = prev_rc.borrow_mut();
                prev_ref.next = next_weak.clone();
            }
        } else {
            self.head = next_weak.clone();
        }

        // set next prev to node prev
        if let Some(ref next) = next_weak {
            if let Some(next_rc) = next.upgrade() {
                let mut next_ref = next_rc.borrow_mut();
                next_ref.prev = prev_weak.clone();
            }
        } else {
            self.tail = prev_weak.clone();
        }

        node_ref.prev = None;
        node_ref.next = None;
    }

    fn push_node_to_head(&mut self, node: Weak<RefCell<ListNode<K, T>>>) {
        self.assert_invariants();
        // if the list is empty set the node to be the head and tail
        if self.is_empty() {
            self.head = Some(node.clone());
            self.tail = Some(node);
            return;
        }

        // upgrade head to Rc to get mutable access
        // set the curr head prev pointer to be a weak referene to the current node
        if let Some(curr_head) = self.head.clone() {
            if let Some(curr_head_rc) = curr_head.upgrade() {
                let mut curr_head_mut = curr_head_rc.as_ref().borrow_mut();
                curr_head_mut.prev = Some(node.clone());
            }
        }

        // upgrade current node to RC to get mutable access
        // set prev to None
        // set next on current node to current head
        if let Some(new_head_rc) = node.upgrade() {
            let mut new_head_mut = new_head_rc.as_ref().borrow_mut();
            new_head_mut.prev = None;
            new_head_mut.next = self.head.clone();
        }

        // promote node to head
        self.head = Some(node)
    }

    fn pop_tail(&mut self) {
        self.assert_invariants();
        if self.tail.is_none() {
            return;
        }

        let curr_tail_weak_ref = self.tail.clone().unwrap();
        let curr_tail_rc = curr_tail_weak_ref.upgrade().unwrap();
        let curr_tail_ref: Ref<ListNode<K, T>> = curr_tail_rc.as_ref().borrow();

        let key_to_pop = curr_tail_ref.key.clone();
        if let Some(new_tail) = curr_tail_ref.prev.clone() {
            let new_tail_rc = new_tail.upgrade().unwrap();
            let mut new_tail_ref = new_tail_rc.borrow_mut();
            new_tail_ref.next = None;
            drop(new_tail_ref);

            self.tail = Some(new_tail)
        } else {
            self.head = None;
            self.tail = None;
        }

        self.node_map.remove(&key_to_pop);
    }

    /// Get method without associated checks for key existance.
    /// Will panic when value does not exist in the map.
    pub fn get_unchecked(&mut self, key: &K) -> T {
        self.assert_invariants();
        let node_rc = self.node_map[key].clone();
        let node_ref: RefMut<ListNode<K, T>> = node_rc.as_ref().borrow_mut();
        let value = node_ref.value.clone();
        let res = value;
        drop(node_ref);

        if let Some(head_clone) = self.head.clone() {
            if let Some(head_rc) = head_clone.upgrade() {
                if !Rc::ptr_eq(&head_rc, &node_rc) {
                    self.unlink_node(Rc::clone(&node_rc));
                    let node_weak_ref = Rc::downgrade(&node_rc);
                    self.push_node_to_head(node_weak_ref);
                }
            }
        }
        res
    }

    /// Fetch value from the cache for associated key.
    /// Key value pair will then be promoted to most recently used.
    /// When they Key does not exist in the cache, None will be returned.
    pub fn get(&mut self, key: &K) -> Option<T> {
        self.assert_invariants();
        if self.head.is_none() {
            return None;
        }
        let node_rc = self.node_map.get(key)?.clone();
        let node_ref: RefMut<ListNode<K, T>> = node_rc.as_ref().borrow_mut();
        let value = node_ref.value.clone();
        let res = Some(value);
        drop(node_ref);

        if let Some(head_clone) = self.head.clone() {
            if let Some(head_rc) = head_clone.upgrade() {
                if !Rc::ptr_eq(&head_rc, &node_rc) {
                    self.unlink_node(Rc::clone(&node_rc));
                    let node_weak_ref = Rc::downgrade(&node_rc);
                    self.push_node_to_head(node_weak_ref);
                }
            }
        }

        res
    }
}
