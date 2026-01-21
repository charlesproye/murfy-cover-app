'use client';

import React from 'react';
import { useAuth } from '@/contexts/AuthContext';
import TableExtremum from '@/components/entities/dashboard/home/table/TableExtremum';
import ChangeFleet from '@/components/common/ChangeFleet';
import { IconTransfer } from '@tabler/icons-react';

const DashboardPage = (): React.ReactElement => {
  const { fleet } = useAuth();

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
