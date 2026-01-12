'use client';

import React, { useState } from 'react';
import DisplayFilterButtons from '@/components/filters/filter-buttons/DisplayFilterButtons';
import FleetsTab from '@/components/entities/fleet-manager/tabs/FleetsTab';
import VehiclesTab from '@/components/entities/fleet-manager/tabs/VehiclesTab';
import ActivateTab from '@/components/entities/fleet-manager/tabs/ActivateTab';
import DeactivateTab from '@/components/entities/fleet-manager/tabs/DeactivateTab';
import { FleetInfo } from '@/interfaces/fleet-manager/fleet-manager';

type TabType = 'Fleets' | 'Vehicles' | 'Activate' | 'Deactivate';

const FleetManagerPage = (): React.ReactElement => {
  const [activeTab, setActiveTab] = useState<TabType>('Fleets');
  const [fleets, setFleets] = useState<FleetInfo[]>([]);
  const [selectedFleetId, setSelectedFleetId] = useState<string | null>(null);

  const tabs: TabType[] = ['Fleets', 'Vehicles', 'Activate', 'Deactivate'];

  return (
    <div className="flex flex-col gap-6 w-full mt-4">
      <div className="flex flex-col gap-2">
        <h1 className="text-3xl font-bold">Fleet Manager</h1>
        <p className="text-gray-600">Activate, deactivate and monitor the status of your vehicles</p>
      </div>

      <div className="flex justify-start">
        <DisplayFilterButtons<TabType>
          selected={activeTab}
          setSelected={setActiveTab}
          filters={tabs}
        />
      </div>

      <div className="w-full">
        {activeTab === 'Fleets' && (
          <FleetsTab
            fleets={fleets}
            setFleets={setFleets}
            setSelectedFleetId={setSelectedFleetId}
            setActiveTab={setActiveTab}
          />
        )}
        {activeTab === 'Vehicles' && (
          <VehiclesTab
            fleets={fleets}
            selectedFleetId={selectedFleetId}
            setSelectedFleetId={setSelectedFleetId}
          />
        )}
        {activeTab === 'Activate' && (
          <ActivateTab
            fleets={fleets}
            selectedFleetId={selectedFleetId}
            setSelectedFleetId={setSelectedFleetId}
          />
        )}
        {activeTab === 'Deactivate' && (
          <DeactivateTab
            fleets={fleets}
            selectedFleetId={selectedFleetId}
            setSelectedFleetId={setSelectedFleetId}
          />
        )}
      </div>
    </div>
  );
};

export default FleetManagerPage;
