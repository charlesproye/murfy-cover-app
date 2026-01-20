'use client';

import DataCardsHome from '@/components/entities/dashboard/home/DataCardHome';

import React, { memo, useState } from 'react';
import { useAuth } from '@/contexts/AuthContext';
import TableExtremum from '@/components/entities/dashboard/home/table/TableExtremum';
import ChangeFleet from '@/components/common/ChangeFleet';
import { IconTransfer } from '@tabler/icons-react';

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
      <div className="w-full flex justify-center">
        <div className="items-center gap-1 relative flex bg-primary/5 border-2 border-primary/20 rounded-xl w-fit p-2 hover:scale-[0.99] transition-transform duration-150">
          <IconTransfer
            color="gray"
            className="w-[22px] h-[22px] sm:w-[18px] sm:h-[18px]"
          />
          <ChangeFleet className="border-primary/30 bg-white" />
        </div>
      </div>
      <div className="w-full bg-white rounded-lg p-6 py-4 relative">
        <TableExtremum fleet={fleet?.id ?? null} />
      </div>
    </div>
  );
};

export default DashboardPage;
