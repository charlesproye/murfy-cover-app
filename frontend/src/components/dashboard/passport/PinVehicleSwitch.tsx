import React from 'react';

interface PinVehicleSwitchProps {
  isPinned: boolean;
  isLoading?: boolean;
  onChange: (checked: boolean) => void;
  label?: string;
}

const PinVehicleSwitch: React.FC<PinVehicleSwitchProps> = ({
  isPinned,
  isLoading,
  onChange,
  label,
}) => (
  <div className="flex justify-between items-center select-none">
    {label && <span className="text-black text-sm mb-1">{label}</span>}
    <label className="flex items-center cursor-pointer select-none relative">
      <input
        type="checkbox"
        className="sr-only peer"
        checked={isPinned}
        onChange={() => onChange(!isPinned)}
        disabled={isLoading}
      />
      <div className="w-11 h-6 bg-white border border-primary/30 peer-checked:bg-primary rounded-full transition-all"></div>
      <div className="absolute left-1 top-1 bg-white w-4 h-4 rounded-full shadow-sm border border-gray/30 peer-checked:border-primary peer-checked:translate-x-5 transition-all"></div>
    </label>
  </div>
);

export default PinVehicleSwitch;
