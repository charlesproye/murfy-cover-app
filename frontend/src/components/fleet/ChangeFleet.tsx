import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import useAuth from '@/contexts/AuthContext';
import { IconTransfer } from '@tabler/icons-react';
import { ReactElement } from 'react';

const ChangeFleet = (): ReactElement => {
  const { isAuthenticated, setFleet, user, fleet } = useAuth();

  const handleChange = (fleetId: string): void => {
    if (!isAuthenticated) return;

    const selectedFleet = user?.fleets.find((fleet) => fleet.id === fleetId);
    if (!selectedFleet) return;

    setFleet(selectedFleet);
  };

  return (
    <div className="items-center gap-1 ml-2 relative flex">
      <IconTransfer color="gray" className="w-[22px] h-[22px] sm:w-[18px] sm:h-[18px]" />
      <Select onValueChange={(value) => handleChange(value)} defaultValue={fleet?.id}>
        <SelectTrigger className="border-none focus:ring-0">
          <SelectValue placeholder="Select a fleet" />
        </SelectTrigger>
        <SelectContent>
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
