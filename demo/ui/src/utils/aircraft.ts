import { AircraftPayload, AircraftType } from '../types';

export function getAircraftType(p: AircraftPayload): AircraftType {
  if (p.on_ground) return 'ground';
  const alt = p.altitude ?? 0;
  const vel = p.velocity ?? 0;
  if (vel < 35 && alt < 800)   return 'helicopter';
  if (vel > 220 && alt > 8000) return 'widebody';
  if (vel > 140)               return 'narrowbody';
  if (vel > 60)                return 'turboprop';
  return 'small';
}

/** Altitude-based color for markers and trails */
export function getAltitudeColor(p: AircraftPayload): string {
  if (p.on_ground) return '#64748b';
  const alt = p.altitude ?? 0;
  if (alt > 10000) return '#a78bfa'; // violet  — high cruise
  if (alt > 7000)  return '#38bdf8'; // sky     — cruise
  if (alt > 3000)  return '#34d399'; // green   — climb/descent
  if (alt > 500)   return '#fbbf24'; // amber   — low
  return '#f87171';                   // red     — very low / unverified
}

export function getTypeImage(type: AircraftType): string {
  const map: Record<AircraftType, string> = {
    widebody:   '/planes/widebody.svg',
    narrowbody: '/planes/narrowbody.svg',
    turboprop:  '/planes/small.svg',
    small:      '/planes/small.svg',
    helicopter: '/planes/helicopter.svg',
    ground:     '/planes/narrowbody.svg',
  };
  return map[type];
}

export function getTypeLabel(type: AircraftType): string {
  const labels: Record<AircraftType, string> = {
    widebody:   'Wide-body',
    narrowbody: 'Narrow-body',
    turboprop:  'Turboprop',
    small:      'Light aircraft',
    helicopter: 'Helicopter',
    ground:     'On ground',
  };
  return labels[type];
}

export function fmtAlt(alt: number | null | undefined): string {
  if (alt == null) return '—';
  if (alt >= 1000) return `${(alt / 1000).toFixed(1)} km`;
  return `${Math.round(alt)} m`;
}

export function fmtSpeed(vel: number | null | undefined): string {
  if (vel == null) return '—';
  const kts = Math.round(vel * 1.944);
  return `${kts} kt`;
}

/** Compute bearing (degrees 0–360) from point A to point B */
export function bearing(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const lat1R = (lat1 * Math.PI) / 180;
  const lat2R = (lat2 * Math.PI) / 180;
  const y = Math.sin(dLon) * Math.cos(lat2R);
  const x = Math.cos(lat1R) * Math.sin(lat2R) - Math.sin(lat1R) * Math.cos(lat2R) * Math.cos(dLon);
  return ((Math.atan2(y, x) * 180) / Math.PI + 360) % 360;
}

/** Country code → flag emoji */
export function countryFlag(country: string | null | undefined): string {
  const map: Record<string, string> = {
    'United States': '🇺🇸', 'Germany': '🇩🇪', 'United Kingdom': '🇬🇧',
    'France': '🇫🇷', 'Netherlands': '🇳🇱', 'China': '🇨🇳', 'Japan': '🇯🇵',
    'Australia': '🇦🇺', 'Canada': '🇨🇦', 'Spain': '🇪🇸', 'Italy': '🇮🇹',
    'Brazil': '🇧🇷', 'India': '🇮🇳', 'Russian Federation': '🇷🇺',
    'South Korea': '🇰🇷', 'Turkey': '🇹🇷', 'Mexico': '🇲🇽',
    'Switzerland': '🇨🇭', 'Sweden': '🇸🇪', 'Norway': '🇳🇴',
    'Denmark': '🇩🇰', 'Finland': '🇫🇮', 'Poland': '🇵🇱',
    'Austria': '🇦🇹', 'Portugal': '🇵🇹', 'Ireland': '🇮🇪',
    'Belgium': '🇧🇪', 'Greece': '🇬🇷', 'Romania': '🇷🇴',
    'Czech Republic': '🇨🇿', 'Hungary': '🇭🇺', 'Singapore': '🇸🇬',
    'United Arab Emirates': '🇦🇪', 'Thailand': '🇹🇭', 'Malaysia': '🇲🇾',
    'Indonesia': '🇮🇩', 'South Africa': '🇿🇦', 'Argentina': '🇦🇷',
    'Chile': '🇨🇱', 'Colombia': '🇨🇴', 'Israel': '🇮🇱',
    'Saudi Arabia': '🇸🇦', 'Qatar': '🇶🇦', 'Hong Kong': '🇭🇰',
    'Taiwan': '🇹🇼', 'New Zealand': '🇳🇿', 'Iceland': '🇮🇸',
  };
  return country ? (map[country] ?? '🌍') : '🌍';
}
