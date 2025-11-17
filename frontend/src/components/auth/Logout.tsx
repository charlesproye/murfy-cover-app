import { IconLogout } from '@tabler/icons-react';
import React from 'react';
import { useAuth } from '@/contexts/AuthContext';

const Logout = (): React.ReactElement => {
  const { handleLogout } = useAuth();

  return (
    <div className="flex gap-2" onClick={handleLogout}>
      <IconLogout size={20} stroke={1.7} color="gray" />
      <p className="text-gray dark:text-dark-gray text-sm font-thin">Logout</p>
    </div>
  );
};

export default Logout;
