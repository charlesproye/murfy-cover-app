export type SearchBarInput = {
  id: string;
  vin: string;
};

export type SearchBarInputRequestSwr = {
  data: SearchBarInput[] | undefined;
  isLoading: boolean;
  error: unknown;
};
