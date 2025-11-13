'use client';

import PinnedVehicleList from '@/components/dashboard/favorites/PinnedVehicleList';
import React from 'react';

const Individual: React.FC = () => {
  return (
    <div className="flex flex-col h-full w-full space-y-8 pt-4">
      <div className="flex flex-wrap justify-center gap-4 mac:justify-between">
        <PinnedVehicleList />
      </div>
    </div>
  );
};

export default Individual;
