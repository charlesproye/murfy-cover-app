'use client';

import {
  IconCarFilled,
  IconHeartbeat,
  IconRoad,
  IconScoreboard,
} from '@tabler/icons-react';
import React from 'react';

const DataGraphGetIcon: React.FC<{ icon: string }> = ({ icon }) => {
  switch (icon) {
    case 'odometer':
      return <IconRoad className="w-8 h-7 text-gray" />;
    case 'soh':
      return <IconHeartbeat className="w-8 h-7 text-gray" />;
    case 'scoreboard':
      return <IconScoreboard className="w-8 h-7 text-gray" />;
    default:
      return <IconCarFilled className="w-8 h-7 text-gray" />;
  }
};

export default DataGraphGetIcon;
