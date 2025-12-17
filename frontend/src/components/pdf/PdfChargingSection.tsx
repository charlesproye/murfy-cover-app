import React, { useEffect, useState } from 'react';
import { View, Text } from '@react-pdf/renderer';
import { LanguageEnum } from '@/components/entities/flash-report/forms/schema';
import { pdfTexts } from '@/components/pdf/pdf_texts';
import {
  CHARGING_DATA,
  getChargeDuration,
  getChargeDurationValue,
  getCostForType,
} from '@/components/entities/dashboard/passport/report/web-report/charging/lib';

const PdfChargingSection: React.FC<{
  brand: string;
  model: string;
  batteryCapacity?: number;
  language?: LanguageEnum;
}> = ({ brand, model, batteryCapacity, language }) => {
  const texts = pdfTexts[language || LanguageEnum.EN];
  const [timePercentages, setTimePercentages] = useState<number[]>([]);

  useEffect(() => {
    if (!batteryCapacity) return;
    const timeValues = CHARGING_DATA.map((item) =>
      getChargeDurationValue(item.power.value, batteryCapacity),
    );
    const maxTime = Math.max(...timeValues);
    const normalizedTimes = timeValues.map((value) => (value / maxTime) * 100);
    setTimePercentages(normalizedTimes);
  }, [batteryCapacity]);

  return (
    <View
      style={{
        backgroundColor: '#fff',
        borderRadius: 12,
        padding: 18,
        width: '100%',
      }}
    >
      <View style={{ marginBottom: 12 }}>
        <Text style={{ fontSize: 14, fontWeight: 500, color: '#111827' }}>
          {texts.charging.title}
        </Text>
      </View>
      <View
        style={{
          display: 'flex',
          flexDirection: 'row',
          borderBottom: '1px solid #E5E7EB',
          marginBottom: 8,
          width: '100%',
          fontSize: 9,
          color: '#A3A3A3',
          fontWeight: 600,
        }}
      >
        <Text style={{ width: '16%', textAlign: 'center' }}>{texts.charging.type}</Text>
        <Text style={{ width: '38%', textAlign: 'center' }}>
          {texts.charging.time_and_cost_of_charge}
        </Text>
        <Text style={{ width: '26%', textAlign: 'center' }}>
          {texts.charging.typical}
        </Text>
        <Text style={{ width: '20%', textAlign: 'center' }}>{texts.charging.power}</Text>
      </View>
      {CHARGING_DATA.map((row, idx) => (
        <View
          key={idx}
          style={{
            flexDirection: 'row',
            alignItems: 'center',
            marginBottom: 12,
            width: '100%',
            fontSize: 10,
            color: '#111827',
          }}
        >
          <Text style={{ width: '16%', textAlign: 'center', marginTop: 6 }}>
            {row.type}
          </Text>
          <View style={{ width: '38%', textAlign: 'center' }}>
            <View
              style={{
                flexDirection: 'row',
                alignItems: 'center',
                justifyContent: 'space-between',
                marginBottom: 2,
                fontWeight: 400,
              }}
            >
              <Text
                style={{
                  marginRight: 12,
                }}
              >
                {getChargeDuration(row.power.value, batteryCapacity)}
              </Text>
              <Text
                style={{
                  fontWeight: 400,
                  marginLeft: 12,
                }}
              >
                {getCostForType(row.type, batteryCapacity)}
              </Text>
            </View>
            <View
              style={{
                height: 7,
                borderRadius: 4,
                backgroundColor: '#E5E7EB',
                width: '100%',
                marginTop: 0,
                marginBottom: 0,
                overflow: 'hidden',
              }}
            >
              <View
                style={{
                  height: 7,
                  borderRadius: 4,
                  backgroundColor: row.color,
                  width: `${timePercentages[idx] || 0}%`,
                }}
              />
            </View>
          </View>
          <Text
            style={{
              width: '26%',
              textAlign: 'center',
              marginTop: 6,
            }}
          >
            {row.outlet}
          </Text>
          <Text
            style={{
              width: '20%',
              textAlign: 'center',
              marginTop: 6,
            }}
          >
            {row.power.label}
          </Text>
        </View>
      ))}
      <View
        style={{
          marginTop: 10,
          border: '1px solid #43ab6a',
          borderRadius: 6,
          backgroundColor: '#E7F9EF',
          padding: 8,
        }}
      >
        <Text style={{ fontSize: 9, color: '#22C55E' }}>
          {texts.charging.explanation(brand, model)}
        </Text>
      </View>
    </View>
  );
};

export default PdfChargingSection;
