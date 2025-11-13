'use client';

import React from 'react';
import { View, StyleSheet, Text } from '@react-pdf/renderer';
import { ScoreCardPropsPdf } from '@/interfaces/dashboard/passport/ScoreCard/ScoreCardProps';

const ScoreCard: React.FC<ScoreCardPropsPdf> = ({ score }) => {
  return (
    <>
      <Text style={styles.label}>Score</Text>
      <View style={styles.progressBar}>
        {[
          { grade: 'A', color: '#22c55e', left: '0' }, // green-500
          { grade: 'B', color: '#4ade80', left: '16.66' }, // green-400
          { grade: 'C', color: '#eab308', left: '33.32' }, // yellow-500
          { grade: 'D', color: '#f97316', left: '49.98' }, // orange-500
          { grade: 'E', color: '#ea580c', left: '66.64' }, // orange-600
          { grade: 'F', color: '#ef4444', left: '83.3' }, // red-500
        ].map(({ grade, color, left }) => (
          <View
            key={grade}
            style={[
              styles.progressSegment,
              { backgroundColor: color, left: `${left}%` },
              score === grade ? styles.activeProgress : {},
            ]}
          >
            {score === grade && <Text style={styles.scoreText}>{grade}</Text>}
          </View>
        ))}
      </View>
    </>
  );
};

const styles = StyleSheet.create({
  progressBar: {
    width: 120,
    height: 24, // Augmenté de 10 à 25
    backgroundColor: '#e2e8f0',
    position: 'relative',
    transform: `rotate(${90}deg)`,
    transformOrigin: 'center',
    left: 180,
  },
  progressSegment: {
    position: 'absolute',
    bottom: 0,
    height: 24, // Augmenté de 10 à 25
    width: '16.66%',
  },
  activeProgress: {
    height: 28, // Augmenté de 14 à 30
    bottom: -2.5,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.5,
    shadowRadius: 4,
    zIndex: 10,
  },
  label: {
    position: 'absolute',
    left: 170,
    top: 0,
    fontSize: 14,
    transform: `rotate(${0}deg)`,
  },
  scoreText: {
    transform: `rotate(${-90}deg)`,
    color: 'white',
    fontSize: 16,
    position: 'absolute',
    right: '30%',
    top: '25%',
  },
});

export default ScoreCard;
