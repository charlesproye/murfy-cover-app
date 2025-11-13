import { useSearchBarReturn } from '@/interfaces/common/search/DataSearch';
import React, { useEffect, useRef, useState } from 'react';

const useSearchBar = (): useSearchBarReturn => {
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [debouncedTerm, setDebouncedTerm] = useState<string>('');
  const timeoutRef = useRef<NodeJS.Timeout | null>(null);

  const handleSearchChange = (e: React.ChangeEvent<HTMLInputElement>): void => {
    const value = e.target.value;
    setSearchTerm(value);

    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }

    timeoutRef.current = setTimeout(() => {
      setDebouncedTerm(value);
    }, 300);
  };

  const handleClear = (): void => {
    setSearchTerm('');
    setDebouncedTerm('');
  };

  useEffect(() => {
    return () => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    };
  }, []);
  return { searchTerm, debouncedTerm, handleSearchChange, handleClear };
};

export default useSearchBar;
