'use client';

import React from 'react';
import {
  FilterData,
  OptionSelectorProps,
} from '@/interfaces/common/optionSelector/DataFilter';

const OptionSelector: React.FC<OptionSelectorProps> = ({
  data,
  selectedValue,
  changeDataOption,
}) => {
  return (
    <div className="flex items-center bg-white rounded-lg p-2 w-[30%] max-w-[344px] min-w-full">
      <label className="text-gray text-[14px] leading-4 font-medium whitespace-nowrap">
        {data.title}:&nbsp;
      </label>
      <select
        value={selectedValue}
        onChange={(event) => changeDataOption(event.target.value)}
        className="w-full rounded-lg focus:outline-hidden text-[14px] leading-4 font-medium"
      >
        <option value="all">All</option>
        {data?.data?.map((filter: FilterData) => (
          <option key={filter.id} value={filter.id}>
            {filter.name}
          </option>
        ))}
      </select>
    </div>
  );
};

export default OptionSelector;
