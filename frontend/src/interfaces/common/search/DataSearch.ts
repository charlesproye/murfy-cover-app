import React from 'react';

export type useSearchBarReturn = {
  searchTerm: string;
  debouncedTerm: string;
  handleSearchChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
  handleClear: () => void;
};
