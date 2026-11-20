export function toAsyncIterable<T>(items: Iterable<T>): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator]() {
      const iterator = items[Symbol.iterator]();
      return { next: () => Promise.resolve(iterator.next()) };
    },
  };
}
