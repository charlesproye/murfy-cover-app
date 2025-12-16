import { IconLogout } from '@tabler/icons-react';
import React from 'react';
import { useAuth } from '@/contexts/AuthContext';

const Logout = (): React.ReactElement => {
  const { handleLogout } = useAuth();

  return (
    <div className="flex gap-2 cursor-pointer text-red-price" onClick={handleLogout}>
      <p className="text-sm font-thin min-w-14">Logout</p>
      <IconLogout size={20} stroke={1.7} />
    </div>
  );
};

export default Logout;
