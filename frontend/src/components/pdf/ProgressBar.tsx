import { ProgressBarProps } from '@/interfaces/pdf/DataPdf';
import { View, Text } from '@react-pdf/renderer';
import React from 'react';

const ProgressBar: React.FC<ProgressBarProps> = ({ percentage, label }) => {
  const validPercentage = percentage ? Math.min(Math.max(0, percentage), 100) : 0;

  return (
    <View
      style={{
        width: '100%',
        height: 20,
        backgroundColor: '#F1F1F1',
        borderRadius: 6,
        position: 'relative',
        overflow: 'hidden',
      }}
    >
      <View
        style={{
          width: `${validPercentage}%`,
          height: '100%',
          backgroundColor: '#007AFF',
          position: 'absolute',
        }}
      />
      <Text
        style={{
          position: 'relative',
          top: 3,
          left: 10,
          fontFamily: 'Poppins',
          fontWeight: 'semibold',
          fontSize: 10,
          color: 'white',
        }}
      >
        {label}
      </Text>
    </View>
  );
};

export default ProgressBar;
