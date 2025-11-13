import React from 'react';
import { View, Text, Image } from '@react-pdf/renderer';
import { InfoVehicleResult } from '@/interfaces/dashboard/passport/infoVehicle';
import { Score } from '@/interfaces/dashboard/passport/ScoreCard/ScoreCardProps';
import { pdfTexts } from './pdf_texts';
import { LanguageEnum } from '../flash-report/forms/schema';

const scoreLabels = [
  { label: 'A', color: '#2d6d49' },
  { label: 'B', color: '#4ADE80' },
  { label: 'C', color: '#FACC15' },
  { label: 'D', color: '#FDBA74' },
  { label: 'E', color: '#FB7185' },
  { label: 'F', color: '#EF4444' },
];

const PdfHeader: React.FC<{ data: InfoVehicleResult }> = ({ data }) => {
  const v = data?.vehicle_info;
  const b = data?.battery_info;
  const score = (v?.score || 'F') as Score;
  const texts = pdfTexts[LanguageEnum.EN];

  // Fonction pour déterminer la couleur du SoH basée sur le score
  const getSohColor = (letterScore: string): string => {
    switch (letterScore) {
      case 'A':
        return '#22C55E'; // green-500
      case 'B':
        return '#4ADE80'; // green-400
      case 'C':
        return '#FACC15'; // yellow-400
      case 'D':
        return '#FB923C'; // orange-400
      case 'E':
        return '#EA580C'; // orange-600
      case 'F':
        return '#EF4444'; // red-500
      default:
        return '#111827'; // black
    }
  };

  return (
    <View
      style={{
        flexDirection: 'row',
        borderRadius: 12,
        overflow: 'hidden',
        minHeight: 120,
        backgroundColor: '#F6F7F9',
      }}
    >
      {/* Partie gauche : image + infos + battery */}
      <View
        style={{
          flex: 2,
          flexDirection: 'column',
          padding: 18,
          backgroundColor: '#e6f7ee',
          justifyContent: 'flex-start',
        }}
      >
        {/* Ligne image + infos véhicule */}
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
              src={v.image || '/car-placeholder.jpg'}
              style={{ width: 80, height: 60, borderRadius: 8, objectFit: 'cover' }}
            />
          </View>
          {/* Infos véhicule */}
          <View style={{ flexDirection: 'column', gap: 2, flex: 1 }}>
            {/* Marque + modèle sur la même ligne */}
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
                {v.brand ? v.brand.toUpperCase() : '-'}
              </Text>
              <Text
                style={{
                  fontSize: 10,
                  fontWeight: 'bold',
                  color: '#111827',
                }}
              >
                {v.model ? v.model.toUpperCase() : '-'}
              </Text>
            </View>
            <View style={{ flexDirection: 'row', gap: 32 }}>
              <View style={{ flexDirection: 'column', gap: 2 }}>
                <Text style={{ fontSize: 7, color: '#A3A3A3', fontWeight: 600 }}>
                  VIN
                </Text>
                <Text style={{ fontSize: 9, color: '#111827', fontWeight: 500 }}>
                  {v.vin || '-'}
                </Text>
                <Text
                  style={{ fontSize: 7, color: '#A3A3A3', fontWeight: 600, marginTop: 6 }}
                >
                  {texts.mileage}
                </Text>
                <Text style={{ fontSize: 9, color: '#111827', fontWeight: 500 }}>
                  {v.mileage ? `${v.mileage} KM` : '-'}
                </Text>
              </View>
              <View style={{ flexDirection: 'column', gap: 2, marginLeft: 32 }}>
                <Text style={{ fontSize: 7, color: '#A3A3A3', fontWeight: 600 }}>
                  {texts.immatriculation}
                </Text>
                <Text style={{ fontSize: 9, color: '#111827', fontWeight: 500 }}>
                  {v.licence_plate || '-'}
                </Text>
                <Text
                  style={{ fontSize: 7, color: '#A3A3A3', fontWeight: 600, marginTop: 6 }}
                >
                  {texts.registration_date}
                </Text>
                <Text style={{ fontSize: 9, color: '#111827', fontWeight: 500 }}>
                  {v.start_date || '-'}
                </Text>
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
            {/* Icône batterie simple (optionnelle, à commenter si non voulue) */}
            {/* <View style={{ width: 14, height: 14, borderRadius: 3, backgroundColor: '#43ab6a', marginRight: 6, position: 'relative', justifyContent: 'center', alignItems: 'center' }}>
              <View style={{ position: 'absolute', right: -2, top: 4, width: 3, height: 6, backgroundColor: '#43ab6a', borderRadius: 1 }} />
            </View> */}
            <Text
              style={{ fontWeight: 700, fontSize: 10, color: '#111827', marginRight: 8 }}
            >
              {texts.battery}
            </Text>
          </View>
          {/* Labels */}
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
          {/* Valeurs */}
          <View style={{ flexDirection: 'row', gap: 12, width: '70%' }}>
            <Text
              style={{ fontSize: 8, color: '#111827', fontWeight: 500, minWidth: 60 }}
            >
              {b?.oem || '-'}
            </Text>
            <Text
              style={{ fontSize: 8, color: '#111827', fontWeight: 500, minWidth: 50 }}
            >
              {b?.chemistry || '-'}
            </Text>
            <Text
              style={{ fontSize: 8, color: '#111827', fontWeight: 500, minWidth: 60 }}
            >
              {b?.capacity ? `${b.capacity} kWh` : '-'}
            </Text>
            <Text
              style={{ fontSize: 8, color: '#111827', fontWeight: 500, minWidth: 70 }}
            >
              {b?.range ? `${b.range} km` : '-'}
            </Text>
            <Text
              style={{ fontSize: 8, color: '#111827', fontWeight: 500, minWidth: 100 }}
            >
              {b?.consumption ? `${b.consumption} kWh/100km` : '-'}
            </Text>
          </View>
        </View>
      </View>
      {/* Fin partie gauche */}
      {/* Partie droite : score */}
      <View
        style={{
          flex: 1,
          backgroundColor: '#e6f7ee',
          alignItems: 'center',
          justifyContent: 'center',
          padding: 0,
          minHeight: 120,
          borderLeftWidth: 2,
          borderLeftColor: '#E5E7EB',
          borderStyle: 'solid',
        }}
      >
        <View style={{ alignItems: 'center', marginTop: 10, width: '100%' }}>
          {/* SoH en gros */}
          <View
            style={{
              alignItems: 'center',
              justifyContent: 'center',
              marginBottom: 10,
            }}
          >
            <Text
              style={{
                fontSize: 10,
                fontWeight: 'bold',
                color: '#111827',
                marginBottom: 2,
              }}
            >
              STATE-OF-HEALTH
            </Text>
            <Text
              style={{
                fontSize: 32,
                fontWeight: 'bold',
                color: getSohColor(score || ''),
                lineHeight: 1,
              }}
            >
              {b?.soh ? `${b.soh}%` : '-'}
            </Text>
          </View>
          {/* Rangée de scores */}
          <View
            style={{
              width: 160,
              flexDirection: 'row',
              alignItems: 'center',
              justifyContent: 'center',
              backgroundColor: '#fff',
              borderRadius: 8,
              padding: 4,
              marginBottom: 10,
            }}
          >
            {scoreLabels.map((s, i) => (
              <View
                key={s.label}
                style={{
                  width: score === s.label ? 32 : 24,
                  height: score === s.label ? 32 : 24,
                  backgroundColor: s.color,
                  borderRadius: 6,
                  alignItems: 'center',
                  justifyContent: 'center',
                  marginRight: i < 5 ? 6 : 0,
                  // borderWidth: score === s.label ? 2 : 0,
                  // borderStyle: 'solid',
                  borderColor: score === s.label ? '#fff' : 'transparent',
                  zIndex: score === s.label ? 2 : 1,
                }}
              >
                <Text
                  style={{
                    color: '#fff',
                    fontWeight: 'bold',
                    fontSize: score === s.label ? 16 : 12,
                  }}
                >
                  {s.label}
                </Text>
              </View>
            ))}
          </View>
          {/* Texte explicatif */}
          <Text
            style={{
              fontSize: 9,
              color: '#111827',
              textAlign: 'center',
              width: 160,
              fontWeight: 500,
              marginTop: 2,
            }}
          >
            {texts.soh_score_explanation}
          </Text>
        </View>
      </View>
    </View>
  );
};

export default PdfHeader;
