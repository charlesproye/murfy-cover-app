import React from 'react';
import {
  IconCaretDownFilled,
  IconCaretUpFilled,
  IconAdjustmentsAlt,
} from '@tabler/icons-react';
import { SortableTableHeaderProps } from '@/interfaces/common/filter/SortableTableHeaderProps';

const SortTableHeader = <ActiveFilterType extends string>({
  label,
  filter,
  activeFilter,
  sortOrder,
  onChangeFilter,
}: SortableTableHeaderProps<ActiveFilterType>): React.ReactNode => {
  return (
    <th
      className="py-4 text-left font-normal cursor-pointer bg-white sticky -top-6 z-10"
      onClick={() => Boolean(filter) && onChangeFilter(filter)}
    >
      <div className="flex items-center space-x-2">
        <div>{label}</div>
        {Boolean(filter) &&
          (activeFilter === filter ? (
            sortOrder === 'asc' ? (
              <IconCaretUpFilled className="h-4 w-4" />
            ) : (
              <IconCaretDownFilled className="h-4 w-4" />
            )
          ) : (
            <IconAdjustmentsAlt className="h-4 w-4" />
          ))}
      </div>
    </th>
  );
};

export default SortTableHeader;
