'use client';

import { IconCalendarDue, IconCar, IconNumber } from '@tabler/icons-react';
import React from 'react';

const VehicleInfoGetIcon: React.FC<{ icon: string }> = ({ icon }) => {
  switch (icon) {
    case 'Make':
      return <IconCar className="text-gray" size={20} stroke={1.5} />;
    case 'VIN':
      return <IconNumber className="text-gray" size={20} stroke={1.5} />;
    case 'Model':
      return <IconCar className="text-gray" size={20} stroke={1.5} />;
    case 'Plate Number':
    case 'Registration Date':
      return <IconCalendarDue className="text-gray" size={20} stroke={1.5} />;
    default:
      return <IconCar className="text-gray" size={20} stroke={1.5} />;
  }
};

export default VehicleInfoGetIcon;
