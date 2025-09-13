using ConsoleApp1;
using FluentAssertions;

namespace TestProject1;

public class ConcurrentKeyedQueueTests
{
    [Fact]
    public void When_EnqueueUniqueKeys_Should_PreserveFIFOOrder()
    {
        // Arrange
        var q = new ConcurrentKeyedQueue<string, int>();

        // Act
        q.Enqueue("a", 1).Should().BeTrue();
        q.Enqueue("b", 2).Should().BeTrue();
        q.Enqueue("c", 3).Should().BeTrue();

        // Assert
        q.Count.Should().Be(3);

        q.TryDequeue(out var v1).Should().BeTrue();
        v1.Should().Be(1);

        q.TryDequeue(out var v2).Should().BeTrue();
        v2.Should().Be(2);

        q.TryDequeue(out var v3).Should().BeTrue();
        v3.Should().Be(3);

        q.TryDequeue(out _).Should().BeFalse();
        q.Count.Should().Be(0);
    }

    [Fact]
    public void When_EnqueueDuplicateKey_Should_ReturnFalse_AndNotChangeOrder()
    {
        // Arrange
        var q = new ConcurrentKeyedQueue<string, int>();

        // Act
        q.Enqueue("a", 1).Should().BeTrue();
        q.Enqueue("b", 2).Should().BeTrue();
        var dup = q.Enqueue("a", 99);

        // Assert
        dup.Should().BeFalse();
        q.Count.Should().Be(2);

        q.TryDequeue(out var v1).Should().BeTrue();
        v1.Should().Be(1);
        q.TryDequeue(out var v2).Should().BeTrue();
        v2.Should().Be(2);
    }

    [Fact]
    public void When_TryDequeueOnEmpty_Should_ReturnFalse()
    {
        // Arrange
        var q = new ConcurrentKeyedQueue<string, int>();

        // Act
        var result = q.TryDequeue(out var value);

        // Assert
        result.Should().BeFalse();
        value.Should().Be(default);
    }

    [Fact]
    public void When_TryRemoveExistingKey_Should_RemoveFromMiddle()
    {
        // Arrange
        var q = new ConcurrentKeyedQueue<string, int>();
        q.Enqueue("a", 1);
        q.Enqueue("b", 2);
        q.Enqueue("c", 3);
        q.Enqueue("d", 4);

        // Act
        q.TryRemove("b", out var removed).Should().BeTrue();

        // Assert
        removed.Should().Be(2);
        q.Count.Should().Be(3);

        // Remaining order should be a, c, d
        q.TryDequeue(out var v1).Should().BeTrue();
        v1.Should().Be(1);
        q.TryDequeue(out var v2).Should().BeTrue();
        v2.Should().Be(3);
        q.TryDequeue(out var v3).Should().BeTrue();
        v3.Should().Be(4);
    }

    [Fact]
    public void When_TryRemoveMissingKey_Should_ReturnFalse()
    {
        // Arrange
        var q = new ConcurrentKeyedQueue<string, int>();
        q.Enqueue("a", 1);

        // Act + Assert
        q.TryRemove("zzz", out var removed).Should().BeFalse();
        removed.Should().Be(default);
        q.Count.Should().Be(1);
    }

    [Fact]
    public void When_TryPeek_Should_NotRemoveItem()
    {
        // Arrange
        var q = new ConcurrentKeyedQueue<string, int>();
        q.Enqueue("a", 1);
        q.Enqueue("b", 2);

        // Act
        q.TryPeek(out var peek).Should().BeTrue();

        // Assert
        peek.Should().Be(1);
        q.Count.Should().Be(2);
    }

    [Fact]
    public void When_Enumerating_Should_UseSnapshot()
    {
        // Arrange
        var q = new ConcurrentKeyedQueue<string, int>();
        q.Enqueue("a", 1);
        q.Enqueue("b", 2);

        // Act: get enumerator (snapshot created), then mutate queue
        var enumerator = q.GetEnumerator();
        q.Enqueue("c", 3);

        var listed = new List<int>();
        while (enumerator.MoveNext())
            listed.Add(enumerator.Current);

        // Assert: snapshot should not contain "c"
        listed.Should().Equal(1, 2);
        q.Count.Should().Be(3);
    }

    [Fact]
    public void When_ContainsKey_Should_ReflectPresence()
    {
        // Arrange
        var q = new ConcurrentKeyedQueue<string, int>();

        // Act + Assert
        q.ContainsKey("a").Should().BeFalse();
        q.Enqueue("a", 1).Should().BeTrue();
        q.ContainsKey("a").Should().BeTrue();
        q.TryRemove("a", out _).Should().BeTrue();
        q.ContainsKey("a").Should().BeFalse();
    }

    [Fact]
    public void When_ManyThreadsEnqueueSameKey_Should_AllowOnlyOne()
    {
        // Arrange
        var q = new ConcurrentKeyedQueue<string, int>();
        int attempts = 128;
        int successCount = 0;

        // Act
        Parallel.For(0, attempts, _ =>
        {
            if (q.Enqueue("same", 42))
                Interlocked.Increment(ref successCount);
        });

        // Assert
        successCount.Should().Be(1);
        q.Count.Should().Be(1);
        q.TryPeek(out var v).Should().BeTrue();
        v.Should().Be(42);
    }

    [Fact]
    public void When_RemovingInParallel_Should_RemoveAllExactlyOnce()
    {
        // Arrange
        var q = new ConcurrentKeyedQueue<int, int>();
        const int N = 1000;
        for (int i = 0; i < N; i++)
            q.Enqueue(i, i).Should().BeTrue();

        // Act
        int removedCount = 0;
        Parallel.For(0, N, i =>
        {
            if (q.TryRemove(i, out var v))
            {
                v.Should().Be(i);
                Interlocked.Increment(ref removedCount);
            }
        });

        // Assert
        removedCount.Should().Be(N);
        q.Count.Should().Be(0);
        q.TryDequeue(out _).Should().BeFalse();
    }
}
