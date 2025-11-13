'use client';

import React, { useState, useRef, useEffect } from 'react';
import { IconChevronDown } from '@tabler/icons-react';
import {
  FilterData,
  OptionSelectorMultipleProps,
} from '@/interfaces/common/optionSelector/DataFilter';

const OptionSelectorMultiple: React.FC<OptionSelectorMultipleProps> = ({
  data,
  selectedValues,
  changeDataOption,
}) => {
  const [isOpen, setIsOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent): void => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const handleCheckboxChange = (value: string): void => {
    let newSelectedValues;
    if (selectedValues.includes(value)) {
      newSelectedValues = selectedValues.filter((v) => v !== value);
    } else {
      newSelectedValues = [...selectedValues, value];
    }
    changeDataOption(newSelectedValues);
  };

  const toggleDropdown = (): void => {
    setIsOpen(!isOpen);
  };

  return (
    <div
      ref={dropdownRef}
      className="relative flex flex-col w-[30%] max-w-[344px] min-w-full"
    >
      <div
        className={`flex items-center justify-between cursor-pointer bg-white rounded-lg p-3
           transition-all duration-200 hover:border-blue-400
          ${isOpen ? 'border-blue-500 ring-2 ring-blue-100' : ''}`}
        onClick={toggleDropdown}
      >
        <div className="flex items-center space-x-2 overflow-hidden">
          <span className="text-gray-600 text-sm font-medium whitespace-nowrap">
            {data.title}
          </span>
          {selectedValues.length > 0 && (
            <span className="bg-blue-100 text-blue-800 text-xs font-medium px-2 py-0.5 rounded-full truncate max-w-[200px]">
              {data.data
                ?.filter((filter) => selectedValues.includes(filter.id))
                .map((filter) => filter.name)
                .join(', ')}
            </span>
          )}
        </div>
        <IconChevronDown
          className={`w-4 h-4 text-gray-500 transition-transform duration-200
            ${isOpen ? 'transform rotate-180' : ''}`}
          stroke={1.5}
        />
      </div>

      {isOpen && (
        <div
          className="absolute top-full left-0 w-full mt-2 bg-white
          rounded-lg shadow-lg z-[1000] max-h-[280px] overflow-y-auto"
        >
          <div className="p-2">
            <label
              className="flex items-center p-2.5 hover:bg-gray-50 cursor-pointer
              rounded-md transition-colors duration-150"
            >
              <input
                type="checkbox"
                checked={selectedValues.length === 0}
                onChange={() => changeDataOption([])}
                className="w-4 h-4 text-blue-600 rounded border-gray-300
                  focus:ring-blue-500 focus:ring-2"
              />
              <span className="ml-3 text-sm font-medium text-gray-700">All</span>
            </label>
            <div className="border-t border-gray-100 my-1"></div>
            {data.data?.map((filter: FilterData) => (
              <label
                key={filter.id}
                className="flex items-center p-2.5 hover:bg-gray-50 cursor-pointer
                  rounded-md transition-colors duration-150"
              >
                <input
                  type="checkbox"
                  checked={selectedValues.includes(filter.id)}
                  onChange={() => handleCheckboxChange(filter.id)}
                  className="w-4 h-4 text-blue-600 rounded border-gray-300
                    focus:ring-blue-500 focus:ring-2"
                />
                <span className="ml-3 text-sm font-medium text-gray-700">
                  {filter.name}
                </span>
              </label>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

export default OptionSelectorMultiple;
