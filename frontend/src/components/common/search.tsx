'use client';

import React, { memo } from 'react';
import { IconSearch, IconX } from '@tabler/icons-react';
import useGetSearchVehicle from '@/interfaces/common/useGetSearchVehicle';
import Link from 'next/link';
import useSearchBar from '@/hooks/dashboard/common/useSearchBar';

const SearchBar: React.FC = (): React.ReactElement => {
  const { searchTerm, debouncedTerm, handleSearchChange, handleClear } = useSearchBar();
  const { data, isLoading } = useGetSearchVehicle(debouncedTerm);

  return (
    <div className="flex items-center relative">
      <div className="relative max-w-80 hidden mobile:flex mobile:items-center">
        {isLoading ? (
          <div className="absolute left-1 top-1/2 transform -translate-y-1/2">
            <div className="animate-spin rounded-full h-5 w-5 border-t-2 border-b-2 border-primary" />
          </div>
        ) : (
          <IconSearch
            className="absolute left-3 top-1/2 transform -translate-y-1/2"
            size={15}
            stroke={2}
            color="gray"
          />
        )}
        <input
          type="text"
          placeholder="Search for vehicle"
          className="tablet:w-[35vh] rounded-md text-gray bg-gray-background h-9 px-3 pl-8 placeholder:text-gray text-sm"
          value={searchTerm}
          onChange={handleSearchChange}
        />
        {searchTerm && (
          <IconX
            className="absolute right-3 top-1/2 transform -translate-y-1/2 cursor-pointer"
            size={15}
            stroke={2}
            color="gray"
            onClick={handleClear}
          />
        )}
      </div>
      {data && (
        <div className="absolute top-10 w-full z-50">
          {data.length > 0 ? (
            <div className="flex flex-col w-full rounded-md bg-white max-h-52 overflow-y-auto p-3 gap-1">
              {data.slice(0, 10).map((item) => (
                <Link
                  href={`/dashboard/passport/${item.vin}`}
                  key={item.id}
                  onClick={handleClear}
                >
                  <div className="hover:bg-white-ghost rounded-md p-2 cursor-pointer">
                    <p className="text-sm">{item.vin}</p>
                  </div>
                </Link>
              ))}
            </div>
          ) : (
            <div className="flex flex-col w-full rounded-md bg-white max-h-52 overflow-y-auto p-3 gap-1">
              <p className="text-sm">No results found</p>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default memo(SearchBar);
