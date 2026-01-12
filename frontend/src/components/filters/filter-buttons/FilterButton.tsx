import { FilterButtonData } from '@/interfaces/common/filter/FilterButtonData';
import React from 'react';

export const FilterButton = ({
  label,
  isSelected,
  onClick,
}: FilterButtonData): React.ReactElement => {
  return (
    <div
      onClick={onClick}
      className={`cursor-pointer transition-colors duration-300 ${isSelected ? 'bg-primary' : 'bg-transparent'} rounded-xl p-2 px-4`}
    >
      <p
        className={`text-sm font-medium select-none transition-colors duration-300 ${isSelected ? 'text-white' : 'text-gray'}`}
      >
        {label}
      </p>
    </div>
  );
};
