import React, { useState } from 'react';
import {
  useConsumptionVehicles,
  useFastChargeVehicles,
  usePinnedVehicles,
} from '@/hooks/pinned/useGetPinnnedVehicle';
import {
  IndividualVehicleConsumption,
  IndividualVehicleFastCharge,
  IndividualVehiclePinned,
} from '@/interfaces/individual/IndividualVehicle';
import { IconChevronDown } from '@tabler/icons-react';
import { useAuth } from '@/contexts/AuthContext';
import useInfiniteScroll from '@/hooks/dashboard/common/useInfiniteScroll';
import PinButton from './PinButton';

const TABS = [
  { label: 'Favorites', key: 'favorites' },
  { label: 'Fast-charge', key: 'fastcharge' },
  { label: 'Consumption', key: 'consumption' },
];

const PinnedVehicleList: React.FC = () => {
  const [activeTab, setActiveTab] = useState('favorites');
  const [isDropdownOpen, setIsDropdownOpen] = useState(false);
  const [inputValue, setInputValue] = useState('');
  const { fleet } = useAuth();
  const fleetID = fleet?.id || '';
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc');
  const [sortConsumptionDirection, setSortConsumptionDirection] = useState<
    'asc' | 'desc'
  >('desc');

  // Scroll infini pour chaque onglet
  const {
    setListRef: setPinnedRef,
    data: pinnedData,
    isLoading: isLoadingPinned,
  } = useInfiniteScroll<IndividualVehiclePinned['data'][0]>({
    fleet: fleetID,
    label: 'pinned',
    fetchFunction: usePinnedVehicles,
  });

  const {
    setListRef: setFastRef,
    data: fastData,
    isLoading: isLoadingFast,
  } = useInfiniteScroll<IndividualVehicleFastCharge['data'][0]>({
    fleet: fleetID,
    label: 'fastcharge',
    fetchFunction: useFastChargeVehicles,
  });

  const {
    setListRef: setConsRef,
    data: consData,
    isLoading: isLoadingCons,
  } = useInfiniteScroll<IndividualVehicleConsumption['data'][0]>({
    fleet: fleetID,
    label: 'consumption',
    fetchFunction: useConsumptionVehicles,
  });

  // Filtrage VIN sur la liste concaténée
  let filteredVehicles:
    | IndividualVehiclePinned['data']
    | IndividualVehicleFastCharge['data']
    | IndividualVehicleConsumption['data'] = [];

  // Fonction de tri pour les fast charge
  const sortFastChargeData = (
    data: IndividualVehicleFastCharge['data'],
  ): IndividualVehicleFastCharge['data'] => {
    return [...data].sort((a, b) => {
      if (sortDirection === 'desc') {
        return b.fastChargeRatio - a.fastChargeRatio;
      }
      return a.fastChargeRatio - b.fastChargeRatio;
    });
  };

  // Fonction de tri pour la consommation
  const sortConsumptionData = (
    data: IndividualVehicleConsumption['data'],
  ): IndividualVehicleConsumption['data'] => {
    return [...data].sort((a, b) => {
      if (sortConsumptionDirection === 'desc') {
        return (b.consumption || 0) - (a.consumption || 0);
      }
      return (a.consumption || 0) - (b.consumption || 0);
    });
  };

  // On s'assure que les données existent avant de filtrer
  if (activeTab === 'favorites' && pinnedData) {
    filteredVehicles = inputValue
      ? pinnedData.filter(
          (v) => v.vin && v.vin.toLowerCase().includes(inputValue.toLowerCase()),
        )
      : pinnedData;
  } else if (activeTab === 'fastcharge' && fastData) {
    const sortedData = sortFastChargeData(fastData);
    filteredVehicles = inputValue
      ? sortedData.filter(
          (v) => v.vin && v.vin.toLowerCase().includes(inputValue.toLowerCase()),
        )
      : sortedData;
  } else if (activeTab === 'consumption' && consData) {
    const sortedData = sortConsumptionData(consData);
    filteredVehicles = inputValue
      ? sortedData.filter(
          (v) => v.vin && v.vin.toLowerCase().includes(inputValue.toLowerCase()),
        )
      : sortedData;
  }

  const isLoading = isLoadingPinned || isLoadingFast || isLoadingCons;
  const activeTabLabel = TABS.find((tab) => tab.key === activeTab)?.label || '';

  return (
    <div className="w-full bg-white rounded-2xl shadow-sm p-6">
      {/* Sélecteur de VIN */}
      <div className="mb-8">
        <label htmlFor="vin" className="block text-sm text-gray mb-2">
          Filter by VIN
        </label>
        <div className="relative">
          <input
            type="text"
            id="vin"
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            placeholder="Enter a VIN"
            className="w-1/8 px-4 py-1 border border-primary rounded-xl text-sm focus:outline-hidden focus:border-primary"
          />
          {isLoading && (
            <div className="absolute right-3 top-1/2 -translate-y-1/2">
              <div className="animate-spin rounded-full h-4 w-4 border-2 border-primary border-t-transparent"></div>
            </div>
          )}
        </div>
      </div>

      {/* Header + Onglets à droite */}
      <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-2 gap-4 sm:gap-0">
        <div>
          <div className="text-gray text-sm">Fleet overview</div>
          <div className="flex items-center gap-2">
            <span className="text-lg font-medium text-black">Global</span>
            <span role="img" aria-label="download">
              ⬇️
            </span>
          </div>
        </div>

        {/* Version mobile : Menu déroulant */}
        <div className="w-full sm:hidden">
          <button
            onClick={() => setIsDropdownOpen(!isDropdownOpen)}
            className="w-full px-4 py-2 bg-white-ghost rounded-xl text-sm text-gray border border-primary/20 flex justify-between items-center"
          >
            <span>{activeTabLabel}</span>
            <span
              className="transform transition-transform duration-200"
              style={{ transform: isDropdownOpen ? 'rotate(180deg)' : 'rotate(0deg)' }}
            >
              <IconChevronDown className="w-4 h-4" />
            </span>
          </button>
          {isDropdownOpen && (
            <div className="absolute z-10 mt-1 w-[290px] bg-white rounded-xl shadow-lg border border-primary/20">
              {TABS.map((tab) => (
                <button
                  key={tab.key}
                  className={`w-full px-4 py-2 text-sm text-left hover:bg-primary/5 ${
                    activeTab === tab.key ? 'text-primary' : 'text-gray'
                  }`}
                  onClick={() => {
                    setActiveTab(tab.key);
                    setIsDropdownOpen(false);
                  }}
                >
                  {tab.label}
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Version desktop : Onglets pill-switch */}
        <div className="hidden sm:flex bg-white-ghost rounded-2xl items-center px-1 py-1 gap-1">
          {TABS.map((tab) => (
            <button
              key={tab.key}
              className={`px-4 py-1 rounded-xl text-sm transition ${
                activeTab === tab.key ? 'bg-primary text-white shadow-sm' : 'text-gray'
              }`}
              onClick={() => setActiveTab(tab.key)}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </div>

      {/* Tableaux spécifiques pour chaque onglet */}
      {activeTab === 'favorites' && (
        <div ref={setPinnedRef} className="w-full max-h-[500px] overflow-y-auto">
          <div className="overflow-x-auto scrollbar-thin scrollbar-thumb-primary/30 scrollbar-track-white rounded-xl">
            <table className="min-w-[600px] w-full text-left table-fixed text-sm">
              <thead>
                <tr className="text-gray font-light">
                  <th className="py-2 font-normal text-md text-center">VIN</th>
                  <th className="py-2 font-normal text-md text-center">
                    Start contract date
                  </th>
                  <th className="py-2 font-normal text-md text-center">Odometer</th>
                  <th className="py-2 font-normal text-md text-center">SoH</th>
                  <th className="py-2 font-normal text-md text-center">
                    SoH/10 000km
                    <span className="relative group ml-1 cursor-pointer">
                      <span className="text-primary">ⓘ</span>
                      <span className="absolute top-full right-0 transform translate-x-[5%] mt-2 w-64 p-2 bg-white text-gray-800 text-xs rounded-lg shadow-lg opacity-0 group-hover:opacity-100 transition-all duration-200 z-10 border border-gray-100">
                        Soh lost per 10 000km based on the odometer and soh
                        <div className="absolute -top-2 right-[15%] transform translate-x-1/2 w-4 h-4 bg-white border-l border-t border-gray-100 rotate-45"></div>
                      </span>
                    </span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {filteredVehicles.length === 0 && !isLoading ? (
                  <tr>
                    <td colSpan={6} className="text-center py-4 text-gray">
                      No vehicle found for this VIN
                    </td>
                  </tr>
                ) : (
                  (filteredVehicles as IndividualVehiclePinned['data'])?.map(
                    (v, index) => (
                      <tr
                        key={index}
                        className={`py-12 ${index % 2 === 0 ? 'bg-primary/5 border-b border-primary/10' : ''}`}
                      >
                        <td className="py-4 text-black text-center">{v.vin}</td>
                        <td className="py-4 text-primary text-center">{v.startDate}</td>
                        <td className="py-4 text-black text-center">{v.odometer}</td>
                        <td className="py-4 text-black text-center">{v.soh}%</td>
                        <td className="py-4 text-black text-center">
                          {v.medianSohLost10000km}
                        </td>
                      </tr>
                    ),
                  )
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {activeTab === 'fastcharge' && (
        <div ref={setFastRef} className="w-full max-h-[500px] overflow-y-auto">
          <div className="overflow-x-auto scrollbar-thin scrollbar-thumb-primary/30 scrollbar-track-white rounded-xl">
            <table className="min-w-[600px] w-full text-left table-fixed text-sm">
              <thead>
                <tr className="text-gray font-light">
                  <th className="py-2 font-normal text-md text-center">VIN</th>
                  <th className="py-2 font-normal text-md text-center">
                    Start contract date
                  </th>
                  <th className="py-2 font-normal text-md text-center">Odometer</th>
                  <th className="py-2 font-normal text-md text-center">SoH</th>
                  <th
                    className="py-2 font-normal text-center cursor-pointer hover:text-primary"
                    onClick={() =>
                      setSortDirection(sortDirection === 'desc' ? 'asc' : 'desc')
                    }
                  >
                    <div className="flex items-center justify-center gap-1">
                      Ratio Fast-charge
                      <span className="text-primary">
                        {sortDirection === 'desc' ? '↓' : '↑'}
                      </span>
                    </div>
                  </th>
                  <th className="py-2 font-normal text-center">Actions</th>
                </tr>
              </thead>
              <tbody>
                {filteredVehicles.length === 0 && !isLoading ? (
                  <tr>
                    <td colSpan={6} className="text-center py-4 text-gray">
                      No vehicle found for this VIN
                    </td>
                  </tr>
                ) : (
                  (filteredVehicles as IndividualVehicleFastCharge['data'])?.map(
                    (v, index) => (
                      <tr
                        key={index}
                        className={`py-12 ${index % 2 === 0 ? 'bg-primary/5 border-b border-primary/10' : ''}`}
                      >
                        <td className="py-4 text-black text-center">{v.vin}</td>
                        <td className="py-4 text-primary text-center">{v.startDate}</td>
                        <td className="py-4 text-black text-center">{v.odometer}</td>
                        <td className="py-4 text-black text-center">{v.soh}%</td>
                        <td className="py-4 text-black text-center">
                          {Math.round(v.fastChargeRatio * 100)}%
                        </td>
                        <td className="py-4 text-center">
                          <PinButton vin={v.vin} />
                        </td>
                      </tr>
                    ),
                  )
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {activeTab === 'consumption' && (
        <div ref={setConsRef} className="w-full max-h-[500px] overflow-y-auto">
          <div className="overflow-x-auto scrollbar-thin scrollbar-thumb-primary/30 scrollbar-track-white rounded-xl">
            <table className="min-w-[700px] w-full text-left table-fixed text-sm">
              <thead>
                <tr className="text-gray font-light">
                  <th className="py-2 font-normal text-md text-center w-1/6">VIN</th>
                  <th className="py-2 font-normal text-md text-center w-1/6">
                    Start contract date
                  </th>
                  <th className="py-2 font-normal text-md text-center w-1/6">Odometer</th>
                  <th className="py-2 font-normal text-md text-center w-1/6">SoH</th>
                  <th
                    className="py-2 font-normal text-center cursor-pointer hover:text-primary w-1/4"
                    onClick={() =>
                      setSortConsumptionDirection(
                        sortConsumptionDirection === 'desc' ? 'asc' : 'desc',
                      )
                    }
                  >
                    <div className="flex items-center justify-center gap-1">
                      Consommation (Wh/km)
                      <span className="text-primary">
                        {sortConsumptionDirection === 'desc' ? '↓' : '↑'}
                      </span>
                    </div>
                  </th>
                  <th className="py-2 font-normal text-center w-1/12">Actions</th>
                </tr>
              </thead>
              <tbody>
                {filteredVehicles.length === 0 && !isLoading ? (
                  <tr>
                    <td colSpan={6} className="text-center py-4 text-gray">
                      No vehicle found for this VIN
                    </td>
                  </tr>
                ) : (
                  (filteredVehicles as IndividualVehicleConsumption['data'])?.map(
                    (v, index) => (
                      <tr
                        key={index}
                        className={`py-12 ${index % 2 === 0 ? 'bg-primary/5 border-b border-primary/10' : ''}`}
                      >
                        <td className="py-4 text-black text-center">{v.vin}</td>
                        <td className="py-4 text-primary text-center">{v.startDate}</td>
                        <td className="py-4 text-black text-center">{v.odometer}</td>
                        <td className="py-4 text-black text-center">{v.soh}%</td>
                        <td className="py-4 text-black text-center">{v.consumption}</td>
                        <td className="py-4 text-center">
                          <PinButton vin={v.vin} />
                        </td>
                      </tr>
                    ),
                  )
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
};

export default PinnedVehicleList;
