'use client';

import DataCardsHome from '@/components/entities/dashboard/home/DataCardHome';

import React, { memo, useState } from 'react';
import { useAuth } from '@/contexts/AuthContext';
import TableBrands from '@/components/entities/dashboard/home/table/TableBrands';
import { GraphTrendline } from '@/components/charts/home/GraphTrendline';
import TableExtremum from '@/components/entities/dashboard/home/table/TableExtremum';
import TablesFleet from '@/components/entities/dashboard/home/table/TableFleet';
import GraphSoH from '@/components/entities/dashboard/home/graph/GraphSoH';
import GraphQuantity from '@/components/entities/dashboard/home/graph/GraphQuantity';
import LoadingSmall from '@/components/common/loading/loadingSmall';

const MemoizedDataCardsHome = memo(DataCardsHome);

const DashboardPage = (): React.ReactElement => {
  const { fleet } = useAuth();
  const [isDisplay, setIsDisplay] = useState<boolean>(false);
  const [clickedData, setClickedData] = useState<string | null>(null);

  const handleDisplayChange = (newDisplay: boolean, newValue: string | null): void => {
    setClickedData(newValue);
    setIsDisplay(newDisplay);
  };

  return (
    <div className="flex flex-col h-full w-full space-y-8 py-4">
      <div className="flex flex-wrap justify-center gap-4 md:justify-center">
        <MemoizedDataCardsHome fleet={fleet} />
      </div>
      <div className="flex flex-col md:flex-row gap-6 md:max-h-96">
        <div className="w-full md:w-[calc(50%-12px)] bg-white rounded-lg overflow-hidden min-h-[378px]">
          {!isDisplay ? (
            <GraphSoH fleet={fleet?.id ?? null} setIsDisplay={handleDisplayChange} />
          ) : (
            <TablesFleet
              fleet={fleet?.id ?? null}
              setIsDisplay={handleDisplayChange}
              label={clickedData}
            />
          )}
        </div>
        <div className="w-full md:w-[calc(50%-12px)] bg-white rounded-lg p-6">
          <GraphQuantity fleet={fleet?.id ?? null} />
        </div>
      </div>
      <div className="w-full bg-white rounded-lg">
        <TableBrands fleet={fleet?.id ?? null} />
      </div>
      <div className="w-full bg-white rounded-lg p-6">
        {fleet && fleet.id ? <GraphTrendline fleet={fleet.id} /> : <LoadingSmall />}
      </div>
      <div className="w-full bg-white rounded-lg p-6 py-4 relative">
        <TableExtremum fleet={fleet?.id ?? null} />
      </div>
    </div>
  );
};

export default DashboardPage;
