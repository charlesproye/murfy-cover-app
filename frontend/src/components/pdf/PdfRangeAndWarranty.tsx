import React, { useMemo } from 'react';
import { View, Text } from '@react-pdf/renderer';
import { LanguageEnum } from '@/components/entities/flash-report/forms/schema';
import { pdfTexts } from '@/components/pdf/pdf_texts';
import { calculateTrendlineY, parseTrendlineEquation } from '@/lib/trendline';
import { formatNumber } from '@/lib/dataDisplay';

interface PdfRangeAndWarrantyProps {
  warranty_km?: number;
  warranty_date?: number;
  mileage?: number;
  start_date?: string;
  range?: number;
  language?: LanguageEnum;
  trendline: string | null;
}

const greenLight = '#2d6d49';
const greenExtraLight = '#2d6d49';
const greenExtraLightBis = '#f0fbf4';
const gray = '#6B7280';
const white = '#fff';
const border = '#E5E7EB';

// --- Calculs pour Range ---
const URBAN_SUMMER_COEF = 1;
const URBAN_WINTER_COEF = 1.5;
const MOTORWAY_SUMMER_COEF = 1.55;
const MOTORWAY_WINTER_COEF = 2;
const MIXED_SUMMER_COEF = 1.25;
const MIXED_WINTER_COEF = 1.75;

const PdfRangeAndWarranty: React.FC<PdfRangeAndWarrantyProps> = ({
  warranty_km,
  warranty_date,
  mileage,
  start_date,
  range,
  language,
  trendline,
}) => {
  const texts = pdfTexts[language || LanguageEnum.EN];

  const formatRange = (range: number | undefined, coefficient: number): string => {
    if (range === undefined || range === null || range === 0) {
      return '-';
    }
    return `${Math.round(range / coefficient)} km`;
  };

  // --- Calculs pour Warranty ---
  const getWarrantyRemaining = (): string => {
    const warrantyKm = warranty_km || 0;
    const currentMileage = mileage || 0;
    const remainingMileage = Math.max(0, warrantyKm - currentMileage);
    return remainingMileage ? `${formatNumber(remainingMileage)} KM` : '-';
  };

  const getWarrantyKmPercentage = (): number => {
    const warrantyKm = warranty_km || 0;
    const currentMileage = mileage || 0;
    if (warrantyKm <= 0) return 0;
    const percentage = ((warrantyKm - currentMileage) / warrantyKm) * 100;
    return Math.max(0, Math.min(100, percentage));
  };

  const getWarrantyTimeRemaining = (): string => {
    const startDateStr = start_date;
    const warrantyYears = warranty_date;
    if (!startDateStr || warrantyYears === undefined || warrantyYears === null) {
      return '-';
    }
    const startDate = new Date(startDateStr);
    if (isNaN(startDate.getTime())) {
      return '-';
    }
    const endDate = new Date(startDate);
    endDate.setFullYear(endDate.getFullYear() + warrantyYears);
    const now = new Date();
    if (now >= endDate) return '0 Y 0 M LEFT';
    const diffTime = endDate.getTime() - now.getTime();
    const diffDays = Math.floor(diffTime / (1000 * 60 * 60 * 24));
    const years = Math.floor(diffDays / 365);
    const months = Math.floor((diffDays % 365) / 30);
    return `${years} Y ${months} M LEFT`;
  };

  const getWarrantyTimePercentage = (): number => {
    const startDateStr = start_date;
    const warrantyYears = warranty_date;
    if (!startDateStr || warrantyYears === undefined || warrantyYears === null) {
      return 0;
    }
    const startDate = new Date(startDateStr);
    if (isNaN(startDate.getTime())) {
      return 0;
    }
    const endDate = new Date(startDate);
    endDate.setFullYear(endDate.getFullYear() + warrantyYears);
    const now = new Date();
    if (now >= endDate) return 0;
    const totalTime = endDate.getTime() - startDate.getTime();
    const elapsedTime = now.getTime() - startDate.getTime();
    const remainingTime = totalTime - elapsedTime;
    const percentage = (remainingTime / totalTime) * 100;
    return Math.max(0, Math.min(100, percentage));
  };

  const { expectedRange, expectedSoh } = useMemo(() => {
    if (!trendline) return { expectedRange: '-', expectedSoh: '-' };
    const coefficientsAvg = parseTrendlineEquation(trendline);

    const expectedSoh = calculateTrendlineY(warranty_km ?? 0, coefficientsAvg);
    return {
      expectedRange: Math.round(((range ?? 0) * expectedSoh) / 100),
      expectedSoh: Math.round(expectedSoh),
    };
  }, [trendline, warranty_km, range]);

  return (
    <View style={{ flexDirection: 'row', gap: 16 }}>
      {/* Bloc Range */}
      <View
        style={{
          flex: 1,
          backgroundColor: white,
          borderRadius: 12,
          padding: 16,
          border: `1px solid ${border}`,
        }}
      >
        <Text style={{ fontSize: 15, fontWeight: 'medium' }}>{texts.range.title}</Text>
        <Text style={{ fontSize: 9, color: gray, marginBottom: 16 }}>
          {texts.range.subtitle}
        </Text>
        {/* Tableau */}
        <View
          style={{
            flexDirection: 'row',
            borderBottom: `1px solid ${border}`,
            paddingBottom: 8,
            marginBottom: 4,
          }}
        >
          <Text style={{ flex: 1, fontSize: 9, color: gray, fontWeight: 'bold' }}>
            {texts.range.usage}
          </Text>
          <Text
            style={{
              flex: 1,
              fontSize: 9,
              color: gray,
              textAlign: 'center',
            }}
          >
            {texts.range.summer}
          </Text>
          <Text
            style={{
              flex: 1,
              fontSize: 9,
              color: gray,
              textAlign: 'center',
            }}
          >
            {texts.range.winter}
          </Text>
        </View>
        {/* Lignes */}
        <View
          style={{
            flexDirection: 'row',
            borderBottom: `1px solid ${border}`,
            paddingVertical: 8,
          }}
        >
          <Text style={{ flex: 1, fontSize: 9 }}>{texts.range.urban}</Text>
          <Text style={{ flex: 1, fontSize: 9, textAlign: 'center' }}>
            {formatRange(range, URBAN_SUMMER_COEF)}
          </Text>
          <Text style={{ flex: 1, fontSize: 9, textAlign: 'center' }}>
            {formatRange(range, URBAN_WINTER_COEF)}
          </Text>
        </View>
        <View
          style={{
            flexDirection: 'row',
            borderBottom: `1px solid ${border}`,
            paddingVertical: 8,
            marginBottom: 4,
            marginTop: 4,
          }}
        >
          <Text style={{ flex: 1, fontSize: 9 }}>{texts.range.motorway}</Text>
          <Text style={{ flex: 1, fontSize: 9, textAlign: 'center' }}>
            {formatRange(range, MOTORWAY_SUMMER_COEF)}
          </Text>
          <Text style={{ flex: 1, fontSize: 9, textAlign: 'center' }}>
            {formatRange(range, MOTORWAY_WINTER_COEF)}
          </Text>
        </View>
        <View style={{ flexDirection: 'row', paddingVertical: 8, marginBottom: 4 }}>
          <Text style={{ flex: 1, fontSize: 9 }}>{texts.range.mixed}</Text>
          <Text style={{ flex: 1, fontSize: 9, textAlign: 'center' }}>
            {formatRange(range, MIXED_SUMMER_COEF)}
          </Text>
          <Text style={{ flex: 1, fontSize: 9, textAlign: 'center' }}>
            {formatRange(range, MIXED_WINTER_COEF)}
          </Text>
        </View>
      </View>
      {/* Bloc Warranty */}
      <View
        style={{
          flex: 1,
          backgroundColor: white,
          borderRadius: 12,
          padding: 16,
          border: `1px solid ${border}`,
        }}
      >
        <Text style={{ fontSize: 15, fontWeight: 'medium' }}>{texts.warranty.title}</Text>
        <Text style={{ fontSize: 9, color: gray, marginBottom: 10 }}>
          {texts.warranty.subtitle}
        </Text>
        {/* Mileage */}
        <View
          style={{
            flexDirection: 'row',
            justifyContent: 'space-between',
            marginBottom: 2,
          }}
        >
          <Text style={{ fontSize: 9, color: gray }}>
            {texts.warranty.remaining_warranty_mileage}
          </Text>
          <Text style={{ fontSize: 9 }}>{getWarrantyRemaining()}</Text>
        </View>
        {/* Barre de progression mileage */}
        <View
          style={{ height: 6, backgroundColor: border, borderRadius: 3, marginBottom: 8 }}
        >
          <View
            style={{
              height: 6,
              backgroundColor: greenExtraLight,
              borderRadius: 3,
              width: `${getWarrantyKmPercentage()}%`,
            }}
          />
        </View>
        {/* Time */}
        {start_date && (
          <>
            <View
              style={{
                flexDirection: 'row',
                justifyContent: 'space-between',
                marginBottom: 2,
              }}
            >
              <Text style={{ fontSize: 9, color: gray }}>
                {texts.warranty.remaining_warranty_time}
              </Text>
              <Text style={{ fontSize: 9 }}>{getWarrantyTimeRemaining()}</Text>
            </View>
            <View
              style={{
                height: 6,
                backgroundColor: border,
                borderRadius: 3,
                marginBottom: 8,
              }}
            >
              <View
                style={{
                  height: 6,
                  backgroundColor: greenExtraLight,
                  borderRadius: 3,
                  width: `${getWarrantyTimePercentage()}%`,
                }}
              />
            </View>
          </>
        )}
        {/* Bloc vert clair pour expected range */}
        {trendline !== null && (
          <>
            <View
              style={{
                backgroundColor: greenExtraLightBis,
                borderRadius: 6,
                padding: 6,
                flexDirection: 'row',
                justifyContent: 'space-between',
                alignItems: 'center',
                marginBottom: 8,
              }}
            >
              <Text style={{ fontSize: 9, color: gray }}>
                {texts.warranty.expected_range_at_warranty_end}
              </Text>
              <Text style={{ fontSize: 9, color: greenLight }}>
                {expectedRange === '-'
                  ? texts.warranty.missing_data
                  : `${expectedRange} KM (${expectedSoh}%)`}
              </Text>
            </View>
            <Text style={{ fontSize: 9, color: gray }}>
              {texts.warranty.expected_range_at_warranty_end_explanation(
                warranty_date || 8,
                formatNumber(warranty_km),
              )}
            </Text>
          </>
        )}
      </View>
    </View>
  );
};

export default PdfRangeAndWarranty;
