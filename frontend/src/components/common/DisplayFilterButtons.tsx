import React from 'react';
import { FilterButton } from './FilterButton';
import { DisplayFilterButtonsData } from '@/interfaces/common/filter/FilterButtonData';

const DisplayFilterButtons = <T extends string>({
  selected,
  setSelected,
  filters,
}: DisplayFilterButtonsData<T>): React.ReactElement => {
  return (
    <div className="flex gap-2 bg-white-ghost rounded-xl h-12 px-4 items-center">
      {filters.map((filter) => (
        <FilterButton
          key={filter}
          label={filter}
          isSelected={selected === filter}
          onClick={() => setSelected(filter)}
        />
      ))}
    </div>
  );
};

export default DisplayFilterButtons;
