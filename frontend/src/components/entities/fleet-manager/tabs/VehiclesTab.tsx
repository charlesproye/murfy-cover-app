'use client';

import React, { useState, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { IconSearch, IconDownload, IconRefresh } from '@tabler/icons-react';
import { toast } from 'sonner';
import fetchWithAuth from '@/services/fetchWithAuth';
import { ROUTES } from '@/routes';
import {
  FleetInfo,
  VehicleStatus,
  VehicleStatusParams
} from '@/interfaces/fleet-manager/fleet-manager';

interface VehiclesTabProps {
  fleets: FleetInfo[];
  selectedFleetId: string | null;
  setSelectedFleetId: (fleetId: string | null) => void;
}

const VehiclesTab: React.FC<VehiclesTabProps> = ({
  fleets,
  selectedFleetId,
  setSelectedFleetId
}) => {
  const [vehicles, setVehicles] = useState<VehicleStatus[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [vinFilter, setVinFilter] = useState('');
  const [messageFilter, setMessageFilter] = useState<string[]>([]);
  const [statusFilter, setStatusFilter] = useState<string>('all'); // 'all', 'activated', 'requested', 'inactive'
  const [makeFilter, setMakeFilter] = useState<string[]>([]);
  const [lastFetchTime, setLastFetchTime] = useState<number | null>(null);

  const CACHE_DURATION = 30000; // 30 seconds

  const getCacheKey = (fleetId: string) => `vehicles_cache_${fleetId}`;
  const getTimestampKey = (fleetId: string) => `vehicles_timestamp_${fleetId}`;

  const loadVehicles = async (forceRefresh: boolean = false) => {
    if (!selectedFleetId) {
      toast.error('Please select a fleet first');
      return;
    }

    const cacheKey = getCacheKey(selectedFleetId);
    const timestampKey = getTimestampKey(selectedFleetId);
    const now = Date.now();

    // Check cache if not forcing refresh
    if (!forceRefresh) {
      const cachedData = localStorage.getItem(cacheKey);
      const cachedTimestamp = localStorage.getItem(timestampKey);

      if (cachedData && cachedTimestamp) {
        const timestamp = parseInt(cachedTimestamp, 10);
        if (now - timestamp < CACHE_DURATION) {
          setVehicles(JSON.parse(cachedData));
          setLastFetchTime(timestamp);
          return;
        }
      }
    }

    setIsLoading(true);
    try {
      const params: VehicleStatusParams = {
        fleet_id: selectedFleetId,
      };

      const queryString = new URLSearchParams(
        Object.entries(params).filter(([_, v]) => v !== undefined) as [string, string][]
      ).toString();

      const response = await fetchWithAuth<VehicleStatus[]>(
        `${ROUTES.VEHICLE_STATUS}?${queryString}`,
        {
          method: 'GET',
        }
      );

      if (response) {
        setVehicles(response);
        setLastFetchTime(now);
        // Store in cache
        localStorage.setItem(cacheKey, JSON.stringify(response));
        localStorage.setItem(timestampKey, now.toString());
        toast.success(`${response.length} vehicles loaded`);
      }
    } catch (error) {
      toast.error('Failed to load vehicles: ' + (error as Error).message);
    } finally {
      setIsLoading(false);
    }
  };

  const refreshVehicles = () => {
    loadVehicles(true);
  };

  // Auto-load vehicles when fleet is selected
  useEffect(() => {
    if (selectedFleetId) {
      loadVehicles();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedFleetId]);

  // Auto-refresh every 30 seconds if data is visible
  useEffect(() => {
    if (!selectedFleetId || vehicles.length === 0) return;

    const interval = setInterval(() => {
      if (lastFetchTime && Date.now() - lastFetchTime >= CACHE_DURATION) {
        loadVehicles(true);
      }
    }, CACHE_DURATION);

    return () => clearInterval(interval);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedFleetId, lastFetchTime, vehicles.length]);

  const downloadCSV = () => {
    if (vehicles.length === 0) {
      toast.error('No data to download');
      return;
    }

    const headers = ['VIN', 'Requested Status', 'Status', 'Message', 'Comment'];
    const rows = vehicles.map(v => [
      v.vin,
      v.requested_status ? 'Yes' : 'No',
      v.status ? 'Yes' : 'No',
      v.message,
      v.comment || ''
    ]);

    const csvContent = [
      headers.join(','),
      ...rows.map(row => row.map(cell => `"${cell}"`).join(','))
    ].join('\n');

    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `vehicles_${selectedFleetId}_${new Date().toISOString().split('T')[0]}.csv`;
    a.click();
    window.URL.revokeObjectURL(url);
  };

  const activatedCount = vehicles.filter(v => v.status).length;
  const requestedCount = vehicles.filter(v => v.requested_status).length;
  const inactiveCount = vehicles.length - activatedCount;

  // Get unique messages and makes for filtering
  const uniqueMessages = Array.from(new Set(vehicles.map(v => v.message)));
  const uniqueMakes = Array.from(new Set(vehicles.map(v => v.make).filter(Boolean))) as string[];

  // Filter vehicles by VIN search, message, status, and make
  const filteredByVin = vinFilter
    ? vehicles.filter(v => v.vin.toLowerCase().includes(vinFilter.toLowerCase()))
    : vehicles;

  const filteredByMessage = messageFilter.length > 0
    ? filteredByVin.filter(v => messageFilter.includes(v.message))
    : filteredByVin;

  const filteredByStatus = statusFilter === 'all'
    ? filteredByMessage
    : statusFilter === 'activated'
    ? filteredByMessage.filter(v => v.status === true)
    : statusFilter === 'requested'
    ? filteredByMessage.filter(v => v.requested_status && !v.status)
    : filteredByMessage.filter(v => !v.status && !v.requested_status);

  const filteredVehicles = makeFilter.length > 0
    ? filteredByStatus.filter(v => v.make && makeFilter.includes(v.make))
    : filteredByStatus;

  // Recalculate counts based on filtered results
  const filteredActivatedCount = filteredVehicles.filter(v => v.status).length;
  const filteredRequestedCount = filteredVehicles.filter(v => v.requested_status).length;
  const filteredInactiveCount = filteredVehicles.length - filteredActivatedCount;

  return (
    <div className="flex flex-col gap-6 w-full">
      {fleets.length === 0 ? (
        <div className="w-full rounded-lg border bg-white p-12 text-center">
          <p className="text-gray-600">
            Please load your fleets in the &quot;Fleets&quot; tab first
          </p>
        </div>
      ) : (
        <>
          <div className="space-y-2">
            <Label htmlFor="fleet-select">Select Fleet</Label>
            <Select value={selectedFleetId || ''} onValueChange={setSelectedFleetId}>
              <SelectTrigger id="fleet-select">
                <SelectValue placeholder="Select a fleet" />
              </SelectTrigger>
              <SelectContent>
                {fleets.map((fleet) => (
                  <SelectItem key={fleet.fleet_id} value={fleet.fleet_id}>
                    {fleet.fleet_name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          {vehicles.length > 0 && (
            <>
              {/* Search Bar and Refresh */}
              <div className="flex gap-4 items-end">
                <div className="flex-1 space-y-2">
                  <div className="relative">
                    <IconSearch className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" size={18} />
                    <Input
                      id="search-vin"
                      placeholder="Search by VIN..."
                      value={vinFilter}
                      onChange={(e) => setVinFilter(e.target.value)}
                      className="pl-10"
                    />
                  </div>
                </div>
                <Button onClick={refreshVehicles} loading={isLoading} variant="outline" size="sm">
                  <IconRefresh size={16} />
                  Refresh
                </Button>
              </div>

              {lastFetchTime && (
                <div className="text-xs text-gray-500 text-right">
                  Last updated: {new Date(lastFetchTime).toLocaleTimeString()}
                  {' • '}
                  Cache expires in {Math.max(0, Math.ceil((CACHE_DURATION - (Date.now() - lastFetchTime)) / 1000))}s
                </div>
              )}

              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div className="rounded-lg border bg-white p-4">
                  <p className="text-sm text-gray-600">Total Vehicles</p>
                  <p className="text-2xl font-bold">{filteredVehicles.length}</p>
                </div>
                <div className="rounded-lg border bg-green-50 p-4">
                  <p className="text-sm text-gray-600">Activated</p>
                  <p className="text-2xl font-bold text-green-600">{filteredActivatedCount}</p>
                </div>
                <div className="rounded-lg border bg-blue-50 p-4">
                  <p className="text-sm text-gray-600">Requested</p>
                  <p className="text-2xl font-bold text-blue-600">{filteredRequestedCount}</p>
                </div>
                <div className="rounded-lg border bg-gray-50 p-4">
                  <p className="text-sm text-gray-600">Inactive</p>
                  <p className="text-2xl font-bold text-gray-600">{filteredInactiveCount}</p>
                </div>
              </div>

              {/* Filters Row */}
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <Select value={statusFilter} onValueChange={setStatusFilter}>
                  <SelectTrigger>
                    <SelectValue placeholder="All statuses" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All statuses</SelectItem>
                    <SelectItem value="activated">Activated</SelectItem>
                    <SelectItem value="requested">Requested</SelectItem>
                    <SelectItem value="inactive">Inactive</SelectItem>
                  </SelectContent>
                </Select>

                {uniqueMakes.length > 0 && (
                  <Select
                    value={makeFilter.length === 1 ? makeFilter[0] : makeFilter.length > 1 ? 'multiple' : 'all'}
                    onValueChange={(value) => {
                      if (value === 'all') {
                        setMakeFilter([]);
                      } else {
                        setMakeFilter([value]);
                      }
                    }}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder="All makes">
                        {makeFilter.length === 0 ? 'All makes' :
                         makeFilter.length === 1 ? makeFilter[0] :
                         `${makeFilter.length} makes selected`}
                      </SelectValue>
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All makes</SelectItem>
                      {uniqueMakes.map((make) => (
                        <SelectItem key={make} value={make}>
                          {make}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                )}

                {uniqueMessages.length > 1 && (
                  <Select
                    value={messageFilter.length === 1 ? messageFilter[0] : messageFilter.length > 1 ? 'multiple' : 'all'}
                    onValueChange={(value) => {
                      if (value === 'all') {
                        setMessageFilter([]);
                      } else {
                        setMessageFilter([value]);
                      }
                    }}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder="All messages">
                        {messageFilter.length === 0 ? 'All messages' :
                         messageFilter.length === 1 ? messageFilter[0] :
                         `${messageFilter.length} messages selected`}
                      </SelectValue>
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All messages</SelectItem>
                      {uniqueMessages.map((msg) => (
                        <SelectItem key={msg} value={msg}>
                          {msg}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                )}
              </div>

              <div className="w-full rounded-lg border bg-white overflow-hidden">
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead className="bg-gray-50">
                      <tr className="border-b">
                        <th className="text-left py-3 px-4 font-medium text-gray-600">VIN</th>
                        <th className="text-left py-3 px-4 font-medium text-gray-600">Make</th>
                        <th className="text-left py-3 px-4 font-medium text-gray-600">Model</th>
                        <th className="text-left py-3 px-4 font-medium text-gray-600">Type</th>
                        <th className="text-left py-3 px-4 font-medium text-gray-600">Version</th>
                        <th className="text-center py-3 px-4 font-medium text-gray-600">Requested</th>
                        <th className="text-center py-3 px-4 font-medium text-gray-600">Activated</th>
                        <th className="text-left py-3 px-4 font-medium text-gray-600">Status</th>
                        <th className="text-left py-3 px-4 font-medium text-gray-600">Comment</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredVehicles.map((vehicle, index) => (
                        <tr
                          key={vehicle.vin}
                          className={`${index === filteredVehicles.length - 1 ? '' : 'border-b'} hover:bg-gray-50 transition-colors`}
                        >
                          <td className="py-3 px-4 font-mono text-xs">{vehicle.vin}</td>
                          <td className="py-3 px-4">{vehicle.make || '-'}</td>
                          <td className="py-3 px-4">{vehicle.model_name || '-'}</td>
                          <td className="py-3 px-4">{vehicle.type || '-'}</td>
                          <td className="py-3 px-4 text-xs text-gray-600">{vehicle.version || '-'}</td>
                          <td className="py-3 px-4 text-center">
                            {vehicle.requested_status ? (
                              <span className="inline-block px-2 py-1 rounded bg-blue-100 text-blue-700 text-xs">
                                Yes
                              </span>
                            ) : (
                              <span className="inline-block px-2 py-1 rounded bg-gray-100 text-gray-600 text-xs">
                                No
                              </span>
                            )}
                          </td>
                          <td className="py-3 px-4 text-center">
                            {vehicle.status ? (
                              <span className="inline-block px-2 py-1 rounded bg-green-100 text-green-700 text-xs">
                                Yes
                              </span>
                            ) : (
                              <span className="inline-block px-2 py-1 rounded bg-gray-100 text-gray-600 text-xs">
                                No
                              </span>
                            )}
                          </td>
                          <td className="py-3 px-4">{vehicle.message}</td>
                          <td className="py-3 px-4 text-gray-600">{vehicle.comment || '-'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </>
          )}
        </>
      )}
    </div>
  );
};

export default VehiclesTab;
