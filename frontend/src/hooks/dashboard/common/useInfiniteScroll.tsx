import {
  InfiniteScrollOptionss,
  UseInfiniteScrollReturns,
} from '@/interfaces/common/scroll/DataScroll';
import { useRef, useCallback, useState, useEffect } from 'react';

// Interface pour les objets qui ont un VIN
interface VehicleWithVin {
  vin: string;
  [key: string]: unknown;
}

const useInfiniteScroll = <T,>({
  fleet,
  label,
  fetchFunction,
}: InfiniteScrollOptionss<T>): UseInfiniteScrollReturns<T> => {
  const listRef = useRef<HTMLDivElement | null>(null);
  const [pages, setPages] = useState<number>(1);
  const [data, setData] = useState<T[]>([]);
  const { data: dataGet, isLoading, hasMorePages } = fetchFunction(fleet, label, pages);

  const handleScroll = (): void => {
    if (listRef.current && hasMorePages) {
      const bottom =
        listRef.current.scrollHeight - 1 <=
        listRef.current.scrollTop + listRef.current.clientHeight;
      if (bottom) {
        setPages((prev) => prev + 1);
      }
    }
  };

  const setListRef = useCallback(
    (node: HTMLDivElement | null) => {
      if (node) {
        listRef.current = node;
        node.addEventListener('scroll', handleScroll);
      } else {
        listRef.current?.removeEventListener('scroll', handleScroll);
      }
    },
    [hasMorePages],
  );

  useEffect(() => {
    if (dataGet?.data) {
      setData((prevData) => {
        // Créer un Map pour dédupliquer basé sur le VIN
        const uniqueMap = new Map<string, T>();

        // Ajouter d'abord les données existantes
        prevData.forEach((item: T) => {
          const vehicle = item as VehicleWithVin;
          if (vehicle.vin) {
            uniqueMap.set(vehicle.vin, item);
          }
        });

        // Ajouter les nouvelles données, en écrasant les doublons
        dataGet.data?.forEach((item: T) => {
          const vehicle = item as VehicleWithVin;
          if (vehicle.vin) {
            uniqueMap.set(vehicle.vin, item);
          }
        });

        // Convertir le Map en tableau
        return Array.from(uniqueMap.values());
      });
    }
  }, [dataGet?.data]);

  return { setListRef, data, isLoading };
};

export default useInfiniteScroll;
