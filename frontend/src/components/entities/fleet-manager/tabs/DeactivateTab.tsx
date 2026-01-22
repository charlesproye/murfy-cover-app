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
import { IconTrash, IconRefresh, IconSearch } from '@tabler/icons-react';
import { toast } from 'sonner';
import fetchWithAuth from '@/services/fetchWithAuth';
import { ROUTES } from '@/routes';
import {
  FleetInfo,
  DeactivationRequest,
  ActivationResponse,
  VehicleStatus,
} from '@/interfaces/fleet-manager/fleet-manager';

interface DeactivateTabProps {
  fleets: FleetInfo[];
  selectedFleetId: string | null;
  setSelectedFleetId: (fleetId: string | null) => void;
}

const DeactivateTab: React.FC<DeactivateTabProps> = ({
  fleets,
  selectedFleetId,
  setSelectedFleetId,
}) => {
  const [isLoading, setIsLoading] = useState(false);
  const [vehicles, setVehicles] = useState<VehicleStatus[]>([]);
  const [selectedVehicles, setSelectedVehicles] = useState<string[]>([]);
  const [results, setResults] = useState<VehicleStatus[] | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [commentFilter, setCommentFilter] = useState('');

  const loadVehicles = async () => {
    if (!selectedFleetId) return;

    setIsLoading(true);
    try {
      const queryString = new URLSearchParams({ fleet_id: selectedFleetId }).toString();
      const response = await fetchWithAuth<VehicleStatus[]>(
        `${ROUTES.VEHICLE_STATUS}?${queryString}`,
        { method: 'GET' },
      );

      if (response) {
        const activeVehicles = response.filter((v) => v.status || v.requested_activation);
        setVehicles(activeVehicles);
        setSelectedVehicles([]);
        setSearchQuery('');

        if (activeVehicles.length === 0) {
          toast.info('No active or requested vehicles found in this fleet');
        } else {
          toast.success(`${activeVehicles.length} active vehicles loaded`);
        }
      }
    } catch (error) {
      toast.error('Failed to load vehicles: ' + (error as Error).message);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    if (selectedFleetId) {
      loadVehicles();
    } else {
      setVehicles([]);
      setSelectedVehicles([]);
      setSearchQuery('');
      setCommentFilter('');
    }
  }, [selectedFleetId]);

  const handleSelectionDeactivation = async () => {
    if (!selectedFleetId) {
      toast.error('Please select a fleet');
      return;
    }
    if (selectedVehicles.length === 0) {
      toast.error('Please select at least one vehicle');
      return;
    }

    setIsLoading(true);
    try {
      const payload: DeactivationRequest = {
        fleet_id: selectedFleetId,
        vins: selectedVehicles, // on peut garder tous les véhicules sélectionnés
      };

      const response = await fetchWithAuth<ActivationResponse>(
        ROUTES.VEHICLE_DEACTIVATE,
        { method: 'DELETE', body: JSON.stringify(payload) },
      );

      if (response && response.vehicles) {
        setResults(response.vehicles);
        toast.success('Vehicles deactivated successfully!');
        await loadVehicles();
      }
    } catch (error) {
      toast.error('Failed to deactivate vehicles: ' + (error as Error).message);
    } finally {
      setIsLoading(false);
    }
  };

  // Filter vehicles based on search query and comment filter
  const filteredVehicles = vehicles.filter((vehicle) => {
    const matchesVin = vehicle.vin.toLowerCase().includes(searchQuery.toLowerCase());
    const matchesComment =
      !commentFilter ||
      (vehicle.comment &&
        vehicle.comment.toLowerCase().includes(commentFilter.toLowerCase()));
    return matchesVin && matchesComment;
  });

  // Only selected vehicles that are currently visible
  const visibleSelectedVehicles = selectedVehicles.filter((vin) =>
    filteredVehicles.some((vehicle) => vehicle.vin === vin),
  );

  const toggleVehicleSelection = (vin: string) => {
    setSelectedVehicles((prev) =>
      prev.includes(vin) ? prev.filter((v) => v !== vin) : [...prev, vin],
    );
  };

  const toggleSelectAll = () => {
    const visibleVins = filteredVehicles.map((v) => v.vin);
    const allVisibleSelected = visibleVins.every((vin) => selectedVehicles.includes(vin));

    if (allVisibleSelected) {
      // Deselect only visible vehicles
      setSelectedVehicles((prev) => prev.filter((vin) => !visibleVins.includes(vin)));
    } else {
      // Select visible vehicles (keep hidden ones)
      setSelectedVehicles((prev) => Array.from(new Set([...prev, ...visibleVins])));
    }
  };

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
            <Label htmlFor="fleet-select-deactivate">Fleet</Label>
            <Select value={selectedFleetId || ''} onValueChange={setSelectedFleetId}>
              <SelectTrigger id="fleet-select-deactivate">
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

          {isLoading && vehicles.length === 0 ? (
            <div className="w-full rounded-lg border bg-white p-12 text-center">
              <p className="text-gray-600">Loading vehicles...</p>
            </div>
          ) : vehicles.length > 0 ? (
            <div className="w-full space-y-4">
              <div className="flex justify-between items-center gap-4">
                <p className="text-sm text-gray-600">
                  {vehicles.length} active or requested vehicles
                </p>
                <Button
                  onClick={loadVehicles}
                  loading={isLoading}
                  variant="outline"
                  size="sm"
                >
                  <IconRefresh size={16} />
                  Refresh
                </Button>
              </div>

              {/* Search Bars */}
              <div className="space-y-2">
                <div className="relative">
                  <IconSearch
                    className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400"
                    size={18}
                  />
                  <Input
                    id="search-vin"
                    placeholder="Search by VIN..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="pl-10"
                  />
                </div>
                <div className="relative">
                  <IconSearch
                    className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400"
                    size={18}
                  />
                  <Input
                    id="search-comment"
                    placeholder="Filter by comment..."
                    value={commentFilter}
                    onChange={(e) => setCommentFilter(e.target.value)}
                    className="pl-10"
                  />
                </div>
              </div>

              <div className="rounded-lg border bg-white p-6 space-y-4">
                <div className="flex justify-between items-center">
                  <Label>
                    Select Vehicles to Deactivate
                    {(searchQuery || commentFilter) && (
                      <span className="ml-2 text-xs text-gray-500">
                        ({filteredVehicles.length} of {vehicles.length})
                      </span>
                    )}
                  </Label>
                  <button
                    onClick={toggleSelectAll}
                    className="text-sm text-primary hover:underline"
                  >
                    {visibleSelectedVehicles.length === filteredVehicles.length &&
                    filteredVehicles.length > 0
                      ? 'Deselect All'
                      : 'Select All'}
                  </button>
                </div>

                <div className="max-h-[400px] overflow-auto border rounded-md">
                  <table className="w-full text-sm">
                    <thead className="bg-gray-50 sticky top-0">
                      <tr className="border-b">
                        <th className="w-12 py-3 px-4"></th>
                        <th className="text-left py-3 px-4 font-medium">VIN</th>
                        <th className="text-center py-3 px-4 font-medium">Status</th>
                        <th className="text-left py-3 px-4 font-medium">Comment</th>
                        <th className="text-left py-3 px-4 font-medium">Message</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredVehicles.length === 0 ? (
                        <tr>
                          <td colSpan={5} className="py-8 text-center text-gray-500">
                            No vehicles found matching your filters
                          </td>
                        </tr>
                      ) : (
                        filteredVehicles.map((vehicle, idx) => (
                          <tr
                            key={vehicle.vin}
                            className={`${idx === filteredVehicles.length - 1 ? '' : 'border-b'} hover:bg-gray-50 cursor-pointer transition-colors`}
                            onClick={() => toggleVehicleSelection(vehicle.vin)}
                          >
                            <td className="py-3 px-4">
                              <input
                                type="checkbox"
                                checked={selectedVehicles.includes(vehicle.vin)}
                                onChange={() => toggleVehicleSelection(vehicle.vin)}
                                className="h-4 w-4 rounded border-gray-300"
                                onClick={(e) => e.stopPropagation()}
                              />
                            </td>
                            <td className="py-3 px-4 font-mono text-xs">{vehicle.vin}</td>
                            <td className="py-3 px-4 text-center">
                              {vehicle.status ? (
                                <span className="inline-block px-2 py-1 rounded bg-green-100 text-green-700 text-xs">
                                  Active
                                </span>
                              ) : vehicle.requested_activation ? (
                                <span className="inline-block px-2 py-1 rounded bg-blue-100 text-blue-700 text-xs">
                                  Requested
                                </span>
                              ) : (
                                <span className="inline-block px-2 py-1 rounded bg-gray-100 text-gray-600 text-xs">
                                  Inactive
                                </span>
                              )}
                            </td>
                            <td className="py-3 px-4 text-sm text-gray-700">
                              {vehicle.comment || (
                                <span className="text-gray-400 italic">No comment</span>
                              )}
                            </td>
                            <td className="py-3 px-4">{vehicle.message}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>

                {selectedVehicles.length > 0 && (
                  <div className="bg-yellow-50 border border-yellow-200 rounded-md p-4 space-y-4">
                    <p className="text-sm text-yellow-800">
                      ⚠️ You are about to deactivate{' '}
                      <span className="font-semibold">
                        {visibleSelectedVehicles.length}
                      </span>{' '}
                      vehicle(s)
                      {selectedVehicles.length !== visibleSelectedVehicles.length && (
                        <span className="text-xs text-gray-500 ml-1">
                          (+{selectedVehicles.length - visibleSelectedVehicles.length}{' '}
                          hidden)
                        </span>
                      )}
                    </p>

                    <Button
                      onClick={handleSelectionDeactivation}
                      loading={isLoading}
                      variant="destructive"
                      className="w-full"
                    >
                      <IconTrash size={18} />
                      Confirm Deactivation
                    </Button>
                  </div>
                )}
              </div>
            </div>
          ) : selectedFleetId ? (
            <div className="w-full rounded-lg border bg-white p-12 text-center">
              <p className="text-gray-600">No active vehicles in this fleet</p>
            </div>
          ) : null}

          {/* Results */}
          {results && results.length > 0 && (
            <div className="w-full rounded-lg border bg-red-50 p-6 space-y-4">
              <h3 className="font-semibold text-lg text-red-800">
                ✅ Deactivation Results
              </h3>

              <div className="overflow-x-auto">
                <table className="w-full text-sm bg-white rounded-md overflow-hidden">
                  <thead className="bg-gray-50">
                    <tr className="border-b">
                      <th className="text-left py-2 px-3 font-medium">VIN</th>
                      <th className="text-center py-2 px-3 font-medium">Status</th>
                      <th className="text-left py-2 px-3 font-medium">Message</th>
                    </tr>
                  </thead>
                  <tbody>
                    {results.map((result, idx) => (
                      <tr key={idx} className="border-b last:border-0">
                        <td className="py-2 px-3 font-mono text-xs">{result.vin}</td>
                        <td className="py-2 px-3 text-center">
                          {!result.status && !result.requested_activation ? (
                            <span className="inline-block px-2 py-1 rounded bg-gray-100 text-gray-700 text-xs">
                              Deactivated
                            </span>
                          ) : (
                            <span className="inline-block px-2 py-1 rounded bg-yellow-100 text-yellow-700 text-xs">
                              Pending
                            </span>
                          )}
                        </td>
                        <td className="py-2 px-3">{result.message}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
};

export default DeactivateTab;
