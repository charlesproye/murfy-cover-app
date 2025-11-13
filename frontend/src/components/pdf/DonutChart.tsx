import { DonutChartProps } from '@/interfaces/pdf/DataPdf';
import { View, Text } from '@react-pdf/renderer';
import React from 'react';

const DonutChart: React.FC<DonutChartProps> = ({ percentage }) => {
  const rotationAngle = (percentage / 100) * 180;

  return (
    <View style={{ position: 'absolute', top: 27 }}>
      <View
        style={{
          position: 'relative',
          width: 180,
          height: 180,
          overflow: 'hidden',
          borderRadius: 100,
          transform: `rotate(${rotationAngle}deg)`,
          transformOrigin: 'center',
        }}
      >
        {/* Fond supérieur avec couleur */}
        <View
          style={{
            position: 'absolute',
            width: '100%',
            height: '50%',
            top: 0,
            left: 0,
            backgroundColor: '#D9D9D9', // Couleur de fond supérieur (gris)
          }}
        />

        {/* Fond inférieur avec couleur et rotation */}
        <View
          style={{
            position: 'absolute',
            width: '100%',
            height: '50%',
            bottom: 0,
            left: 0,
            backgroundColor: '#007AFF', // Couleur de fond inférieur (gris)
          }}
        />

        {/* Cercle blanc au centre */}
        <View
          style={{
            position: 'absolute',
            width: 150,
            height: 150,
            left: 15,
            top: 15,
            backgroundColor: 'white',
            borderRadius: 85,
          }}
        />
      </View>
      <View
        style={{
          position: 'absolute',
          top: 60,
          left: 45,
          flexDirection: 'column',
          justifyContent: 'center',
          alignItems: 'center',
        }}
      >
        <Text
          style={{
            fontFamily: 'Poppins',
            fontWeight: 'medium',
            fontSize: 25,
            margin: -10,
          }}
        >
          {percentage}%
        </Text>
        <Text style={{ fontSize: 12 }}>Remaining range</Text>
      </View>
    </View>
  );
};

export default DonutChart;
