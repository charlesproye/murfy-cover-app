import React from 'react';
import { View, Text, Image } from '@react-pdf/renderer';
import { GetGenerationInfo } from '@/interfaces/flash-reports';
import { pdfTexts } from '@/components/pdf/pdf_texts';
import { LanguageEnum } from '@/components/entities/flash-report/forms/schema';
import { DEFAULT_CAR_IMAGE } from '@/lib/staticData';

const PdfHeaderFlash: React.FC<{ data: GetGenerationInfo }> = ({ data }) => {
  const vehicleInfo = data?.vehicle_info;
  const batteryInfo = data?.battery_info;
  const texts = pdfTexts[data?.language || LanguageEnum.EN];

  return (
    <View
      style={{
        flexDirection: 'row',
        borderRadius: 12,
        overflow: 'hidden',
        minHeight: 120,
        backgroundColor: '#F6F7F9',
        border: '2px solid #2d6d49',
      }}
    >
      <View
        style={{
          flex: 2,
          flexDirection: 'column',
          padding: 18,
          backgroundColor: '#e6f7ee',
          justifyContent: 'flex-start',
        }}
      >
        <View style={{ flexDirection: 'row', alignItems: 'center' }}>
          <View
            style={{
              width: 90,
              height: 90,
              backgroundColor: '#fff',
              borderRadius: 12,
              justifyContent: 'center',
              alignItems: 'center',
              marginRight: 18,
              border: '1px solid #E5E7EB',
            }}
          >
            <Image
              src={vehicleInfo?.image_url || DEFAULT_CAR_IMAGE}
              style={{ width: 80, height: 60, borderRadius: 8, objectFit: 'cover' }}
            />
          </View>
          <View style={{ flexDirection: 'column', gap: 2, flex: 1 }}>
            <View
              style={{
                flexDirection: 'row',
                alignItems: 'center',
                gap: 6,
                marginBottom: 6,
              }}
            >
              <Text
                style={{
                  fontSize: 10,
                  fontWeight: 'bold',
                  color: '#111827',
                  textTransform: 'uppercase',
                  letterSpacing: 0.5,
                }}
              >
                {vehicleInfo?.brand ? vehicleInfo?.brand.toUpperCase() : '-'}
              </Text>
              <Text
                style={{
                  fontSize: 10,
                  fontWeight: 'bold',
                  color: '#111827',
                }}
              >
                {vehicleInfo?.model ? vehicleInfo?.model.toUpperCase() : '-'}
              </Text>
            </View>
            <View style={{ flexDirection: 'row', gap: 32 }}>
              <View style={{ flexDirection: 'column', gap: 2 }}>
                <Text style={{ fontSize: 7, color: '#A3A3A3', fontWeight: 600 }}>
                  VIN
                </Text>
                <Text style={{ fontSize: 9, color: '#111827', fontWeight: 500 }}>
                  {vehicleInfo?.vin || '-'}
                </Text>
                <Text
                  style={{ fontSize: 7, color: '#A3A3A3', fontWeight: 600, marginTop: 6 }}
                >
                  {texts.mileage}
                </Text>
                <Text style={{ fontSize: 9, color: '#111827', fontWeight: 500 }}>
                  {vehicleInfo?.mileage ? `${vehicleInfo?.mileage} KM` : '-'}
                </Text>
              </View>
              <View style={{ flexDirection: 'column', gap: 2, marginLeft: 32 }}>
                <Text style={{ fontSize: 7, color: '#A3A3A3', fontWeight: 600 }}>
                  {texts.type}
                </Text>
                <Text style={{ fontSize: 9, color: '#111827', fontWeight: 500 }}>
                  {vehicleInfo?.type ? vehicleInfo?.type.toUpperCase() : '-'}
                </Text>
                {vehicleInfo?.version && (
                  <>
                    <Text
                      style={{
                        fontSize: 7,
                        color: '#A3A3A3',
                        fontWeight: 600,
                        marginTop: 6,
                      }}
                    >
                      {texts.version}
                    </Text>
                    <Text style={{ fontSize: 9, color: '#111827', fontWeight: 500 }}>
                      {vehicleInfo?.version.toUpperCase()}
                    </Text>
                  </>
                )}
              </View>
            </View>
          </View>
        </View>
        {/* Section Battery multi-lignes comme sur la deuxième image */}
        <View style={{ marginTop: 18, marginLeft: 0 }}>
          <View
            style={{
              flexDirection: 'row',
              alignItems: 'center',
              marginBottom: 4,
            }}
          >
            <Text
              style={{ fontWeight: 700, fontSize: 10, color: '#111827', marginRight: 8 }}
            >
              {texts.battery}
            </Text>
          </View>
          <View style={{ flexDirection: 'row', gap: 12, marginBottom: 2, width: '70%' }}>
            <Text
              style={{ fontSize: 8, color: '#A3A3A3', fontWeight: 600, minWidth: 60 }}
            >
              {texts.battery_oem}
            </Text>
            <Text
              style={{ fontSize: 8, color: '#A3A3A3', fontWeight: 600, minWidth: 50 }}
            >
              {texts.chemistry}
            </Text>
            <Text
              style={{ fontSize: 8, color: '#A3A3A3', fontWeight: 600, minWidth: 60 }}
            >
              {texts.capacity}
            </Text>
            <Text
              style={{ fontSize: 8, color: '#A3A3A3', fontWeight: 600, minWidth: 70 }}
            >
              {texts.wltp_range}
            </Text>
            <Text
              style={{ fontSize: 8, color: '#A3A3A3', fontWeight: 600, minWidth: 100 }}
            >
              {texts.consumption}
            </Text>
          </View>
          <View style={{ flexDirection: 'row', gap: 12, width: '70%' }}>
            <Text
              style={{ fontSize: 8, color: '#111827', fontWeight: 500, minWidth: 60 }}
            >
              {batteryInfo?.oem || '-'}
            </Text>
            <Text
              style={{ fontSize: 8, color: '#111827', fontWeight: 500, minWidth: 50 }}
            >
              {batteryInfo?.chemistry || '-'}
            </Text>
            <Text
              style={{ fontSize: 8, color: '#111827', fontWeight: 500, minWidth: 60 }}
            >
              {batteryInfo?.capacity ? `${batteryInfo?.capacity} kWh` : '-'}
            </Text>
            <Text
              style={{ fontSize: 8, color: '#111827', fontWeight: 500, minWidth: 70 }}
            >
              {batteryInfo?.range ? `${batteryInfo?.range} km` : '-'}
            </Text>
            <Text
              style={{ fontSize: 8, color: '#111827', fontWeight: 500, minWidth: 100 }}
            >
              {batteryInfo?.consumption
                ? `${batteryInfo?.consumption.toFixed(1)} kWh/100km`
                : '-'}
            </Text>
          </View>
        </View>
      </View>

      <View
        style={{
          flex: 1,
          width: '100%',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          backgroundColor: '#2d6d49',
          color: 'white',
          minHeight: 120,
          paddingTop: '20px',
        }}
      >
        <Text
          style={{
            fontSize: 18,
            fontWeight: 'bold',
            marginBottom: 2,
          }}
        >
          STATE-OF-HEALTH
        </Text>
        <View
          style={{
            width: '100%',
            minHeight: 100,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          <Text
            style={{
              fontSize: 42,
              fontWeight: 'bold',
              lineHeight: 1,
            }}
          >
            {batteryInfo?.soh_bib ? `${(batteryInfo?.soh_bib * 100).toFixed(0)}%` : '--'}
          </Text>
        </View>
        {/* Texte explicatif */}
        {/* <Text
            style={{
              fontSize: 9,
              color: '#111827',
              textAlign: 'center',
              width: 160,
              fontWeight: 500,
              marginTop: 2,
            }}
          >
            This SoH value is a statistic estimation based on the vehicle model
            observations.
          </Text> */}
      </View>
    </View>
  );
};

export default PdfHeaderFlash;
