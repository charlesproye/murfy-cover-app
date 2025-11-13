import React from 'react';

export type DataOption = {
  dataOption: string;
  changeDataOption: (newDataCard: string) => void;
};

export type DatacardDisplayOption = {
  formattedData: string;
  variation: number | null;
  title: string;
  getIcon: React.ReactNode;
  isScalarVariation?: boolean;
  actionElement?: React.ReactNode;
};

export type DataCardResult = {
  week?: string;
  avg_odometer: number | null;
  avg_soh: number | null;
  vehicle_count: number;
  pinned_count?: number;
};
