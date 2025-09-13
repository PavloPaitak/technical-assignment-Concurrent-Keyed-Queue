namespace ConsoleApp1;

using System;
using System.Collections;
using System.Collections.Generic;

public sealed class ConcurrentKeyedQueue<TKey, T> : IEnumerable<T>
{
    private sealed class Node
    {
        public TKey Key = default!;
        public T Value = default!;
        public Node? Prev;
        public Node? Next;

        public void Reset()
        {
            Key = default!;
            Value = default!;
            Prev = null;
            Next = null;
        }
    }

    private readonly object _lock = new object();
    private readonly Dictionary<TKey, Node> _index;
    private readonly Node _head; // sentinel (oldest is _head.Next)
    private readonly Node _tail; // sentinel (newest is _tail.Prev)

    // Simple node pool to reduce GC pressure (used only under _lock)
    private Node? _pool; // single-linked stack via Next
    private int _poolCount;
    private readonly int _poolMax;

    public ConcurrentKeyedQueue(int capacity = 0, IEqualityComparer<TKey>? comparer = null, int poolMax = 1024)
    {
        _index = new Dictionary<TKey, Node>(capacity, comparer);
        _head = new Node();
        _tail = new Node();
        _head.Next = _tail;
        _tail.Prev = _head;
        _poolMax = Math.Max(0, poolMax);
    }

    public int Count
    {
        get { lock (_lock) return _index.Count; }
    }

    public bool ContainsKey(TKey key)
    {
        lock (_lock) return _index.ContainsKey(key);
    }

    /// <summary>
    /// Adds an item at the tail of the queue. Returns false if the key already exists.
    /// O(1).
    /// </summary>
    public bool Enqueue(TKey key, T value)
    {
        lock (_lock)
        {
            if (_index.ContainsKey(key))
                return false;

            var node = RentNode();
            node.Key = key;
            node.Value = value;

            // insert before tail (append)
            InsertBefore(_tail, node);
            _index.Add(key, node);
            return true;
        }
    }

    /// <summary>
    /// Removes and returns the oldest item. Returns false if the queue is empty.
    /// O(1).
    /// </summary>
    public bool TryDequeue(out T value)
    {
        lock (_lock)
        {
            var first = _head.Next!;
            if (first == _tail)
            {
                value = default!;
                return false;
            }

            RemoveNode(first);
            _index.Remove(first.Key);
            value = first.Value;
            ReturnNode(first);
            return true;
        }
    }

    /// <summary>
    /// Removes an item by key from anywhere in the queue. Returns false if not found.
    /// O(1).
    /// </summary>
    public bool TryRemove(TKey key, out T value)
    {
        lock (_lock)
        {
            if (!_index.TryGetValue(key, out var node))
            {
                value = default!;
                return false;
            }

            RemoveNode(node);
            _index.Remove(key);
            value = node.Value;
            ReturnNode(node);
            return true;
        }
    }

    /// <summary>
    /// Peeks the oldest item without removing it. Returns false if empty.
    /// O(1).
    /// </summary>
    public bool TryPeek(out T value)
    {
        lock (_lock)
        {
            var first = _head.Next!;
            if (first == _tail)
            {
                value = default!;
                return false;
            }
            value = first.Value;
            return true;
        }
    }

    /// <summary>
    /// Snapshot enumeration: builds a T[] under a short lock and iterates it without locks.
    /// </summary>
    public IEnumerator<T> GetEnumerator()
    {
        // Take snapshot eagerly (right now), not on first MoveNext()
        T[] snapshot;
        lock (_lock)
        {
            snapshot = new T[_index.Count];
            int i = 0;
            for (var n = _head.Next; n != null && n != _tail; n = n!.Next)
                snapshot[i++] = n!.Value;
        }

        return ((IEnumerable<T>)snapshot).GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    // -------------------- internals --------------------

    private void InsertBefore(Node where, Node node)
    {
        node.Prev = where.Prev;
        node.Next = where;
        where.Prev!.Next = node;
        where.Prev = node;
    }

    private static void RemoveNode(Node node)
    {
        var p = node.Prev!;
        var n = node.Next!;
        p.Next = n;
        n.Prev = p;
        node.Prev = null;
        node.Next = null;
    }

    private Node RentNode()
    {
        if (_pool is { } n)
        {
            _pool = n.Next;
            _poolCount--;
            n.Next = null;
            return n;
        }
        return new Node();
    }

    private void ReturnNode(Node node)
    {
        node.Reset();
        if (_poolCount >= _poolMax) return;
        node.Next = _pool;   // reuse Next as pool link
        _pool = node;
        _poolCount++;
    }
}

