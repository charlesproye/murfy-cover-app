import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import useAuth, { FLEET_ALL } from '@/contexts/AuthContext';
import { ReactElement } from 'react';
import { cn } from '@/lib/staticData';

const ChangeFleet = ({ className }: { className?: string }): ReactElement => {
  const { isAuthenticated, setFleet, user, fleet } = useAuth();

  const handleChange = (fleetId: string): void => {
    if (!isAuthenticated) return;

    const selectedFleet = user?.fleets.find((fleet) => fleet.id === fleetId);
    if (!selectedFleet && fleetId !== FLEET_ALL.id) return;

    setFleet(selectedFleet ?? FLEET_ALL);
  };

  return (
    <div className={cn('relative border border-2 rounded-md py-2 w-72', className)}>
      <label
        className="absolute -top-2.5 left-3 px-2 text-sm text-gray rounded-lg"
        style={{ backgroundColor: 'color-mix(in oklab, var(--color-primary) 5%, white)' }}
      >
        Fleet
      </label>
      <Select onValueChange={(value) => handleChange(value)} defaultValue={fleet?.id}>
        <SelectTrigger className="border-none focus:ring-0 w-full shadow-none font-medium text-base">
          <SelectValue placeholder="Select a fleet" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem key={FLEET_ALL.id} value={FLEET_ALL.id} className="font-semibold">
            {FLEET_ALL.name}
          </SelectItem>
          {user?.fleets.map((fleet) => (
            <SelectItem key={fleet.id} value={fleet.id}>
              {fleet.name}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
};

export default ChangeFleet;
