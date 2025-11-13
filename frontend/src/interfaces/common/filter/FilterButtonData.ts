import React from 'react';

export type FilterButtonData = {
  label: string;
  isSelected: boolean;
  onClick: () => void;
};

export type DisplayFilterButtonsData<T extends string> = {
  selected: T | '';
  setSelected: React.Dispatch<React.SetStateAction<T>>;
  filters: T[];
};
