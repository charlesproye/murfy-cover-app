'use client';

import React, { useState } from 'react';
import TableBatteryInfo from '@/components/dashboard/global/TableBatteryInfo';
import DataCards from '@/components/dashboard/global/datacard/DataCards';
import ListFilter from '@/components/common/optionSelector/ListFilter';
import { SelectedFilter } from '@/interfaces/common/optionSelector/DataFilter';
import GraphTrendlineGlobal from '@/components/dashboard/global/GraphTrendlineGlobal';

const Global: React.FC = () => {
  const [selectedFilters, setSelectedFilters] = useState<SelectedFilter[]>([]);

  return (
    <div className="flex flex-col h-full w-full space-y-8 pt-4">
      <div className="flex flex-col gap-8">
        <ListFilter
          selectedFilters={selectedFilters}
          onFiltersChange={setSelectedFilters}
        />
      </div>
      <div className="flex flex-wrap justify-center gap-4 mac:justify-between">
        <DataCards filters={selectedFilters} />
      </div>
      <div className="w-full max-h-[55%] h-auto">
        <TableBatteryInfo filters={selectedFilters} />
      </div>
      <div className="w-full max-h-[55%] h-auto">
        <GraphTrendlineGlobal filters={selectedFilters} />
      </div>
    </div>
  );
};

export default Global;
