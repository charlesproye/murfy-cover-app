'use client';

import React, { useState, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import { IconRefresh, IconChevronRight } from '@tabler/icons-react';
import { toast } from 'sonner';
import fetchWithAuth from '@/services/fetchWithAuth';
import { ROUTES } from '@/routes';
import { FleetInfo, FleetsResponse } from '@/interfaces/fleet-manager/fleet-manager';

interface FleetsTabProps {
  fleets: FleetInfo[];
  setFleets: (fleets: FleetInfo[]) => void;
  setSelectedFleetId: (fleetId: string | null) => void;
  setActiveTab: (tab: 'Fleets' | 'Vehicles' | 'Activate' | 'Deactivate') => void;
}

const FleetsTab: React.FC<FleetsTabProps> = ({
  fleets,
  setFleets,
  setSelectedFleetId,
  setActiveTab,
}) => {
  const [isLoading, setIsLoading] = useState(false);

  const loadFleets = async () => {
    setIsLoading(true);
    try {
      const response = await fetchWithAuth<FleetsResponse>(ROUTES.VEHICLE_FLEETS, {
        method: 'GET',
      });

      if (response && response.fleets) {
        setFleets(response.fleets);
        toast.success(`${response.fleets.length} fleets loaded`);

        // Auto-select first fleet if available
        if (response.fleets.length > 0) {
          setSelectedFleetId(response.fleets[0].fleet_id);
        }
      }
    } catch (error) {
      toast.error('Failed to load fleets: ' + (error as Error).message);
    } finally {
      setIsLoading(false);
    }
  };

  // Auto-load fleets on mount
  useEffect(() => {
    if (fleets.length === 0) {
      loadFleets();
    }
  }, [fleets.length]);

  const handleFleetClick = (fleetId: string) => {
    setSelectedFleetId(fleetId);
    setActiveTab('Vehicles');
    toast.success('Fleet selected');
  };

  return (
    <div className="flex flex-col gap-6 w-full">
      <div className="flex justify-between items-center">
        <div>
          <p className="text-sm text-gray-600">
            Total fleets: <span className="font-semibold">{fleets.length}</span>
          </p>
        </div>
        <Button onClick={loadFleets} loading={isLoading} variant="outline">
          <IconRefresh size={18} />
          Refresh
        </Button>
      </div>

      {isLoading && fleets.length === 0 ? (
        <div className="w-full rounded-lg border bg-white p-12 text-center">
          <p className="text-gray-600">Loading fleets...</p>
        </div>
      ) : fleets.length > 0 ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {fleets.map((fleet) => (
            <div
              key={fleet.fleet_id}
              onClick={() => handleFleetClick(fleet.fleet_id)}
              className="group relative rounded-lg border bg-white p-6 cursor-pointer transition-all hover:shadow-lg hover:border-primary hover:-translate-y-1"
            >
              <div className="flex items-start justify-between">
                <div className="flex-1">
                  <h3 className="text-lg font-semibold text-gray-900 group-hover:text-primary transition-colors">
                    {fleet.fleet_name}
                  </h3>
                </div>
                <IconChevronRight
                  size={20}
                  className="text-gray-400 group-hover:text-primary group-hover:translate-x-1 transition-all flex-shrink-0 ml-2"
                />
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div className="w-full rounded-lg border bg-white p-12 text-center">
          <p className="text-gray-600">No fleets available</p>
        </div>
      )}
    </div>
  );
};

export default FleetsTab;
