export type FetchTableFunction<T> = (
  fleet: string | null,
  label: string | null,
  currentPage: number,
) => {
  data?: { data?: T[] | undefined };
  isLoading: boolean;
  error: unknown;
  hasMorePages: boolean;
};

export type InfiniteScrollOptionss<T> = {
  fleet: string | null;
  label: string | null;
  fetchFunction: FetchTableFunction<T>;
};

export type UseInfiniteScrollReturns<T> = {
  setListRef: (node: HTMLDivElement | null) => void;
  data: T[];
  isLoading: boolean;
};
