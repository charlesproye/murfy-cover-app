'use client';

import {
  IconCarFilled,
  IconHeartbeat,
  IconRoad,
  IconAlertCircle,
} from '@tabler/icons-react';
import React from 'react';

const DataCardGetIcon: React.FC<{ icon: string }> = ({ icon }) => {
  switch (icon) {
    case 'odometer':
      return <IconRoad className="w-8 h-7 text-gray" />;
    case 'soh':
      return <IconHeartbeat className="w-8 h-7 text-gray" />;
    case 'vehicle':
      return <IconCarFilled className="w-8 h-7 text-gray" />;
    case 'f_count':
      return <IconAlertCircle className="w-8 h-7 text-gray" />;
    default:
      return <IconCarFilled className="w-8 h-7 text-gray" />;
  }
};

export default DataCardGetIcon;
