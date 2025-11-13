export type ApiGetRequestSwr<T> = {
  data: T[] | undefined;
  isLoading: boolean;
  error: unknown;
};
