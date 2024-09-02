/*
 * Location lookup
 * v1.0
 * By Arpad Kiss <arpad@greeneyes.ai>
 */

import * as fs from "fs";
import path from "path";
import * as readline from "readline";

export class CSVParser<T extends object> {
  constructor(protected buffer: string[], protected separator = ",") {}

  public getDataMapped(): T[] {
    const data: T[] = [];
    const props: string[] = this.buffer[0]
      .split(this.separator)
      .map((c) => c.replaceAll(`"`, ``).trim());
    for (let i = 1; i < this.buffer.length; i++) {
      const cols = this.buffer[i]
        .split(this.separator)
        .map((c) => c.replaceAll(`"`, ``).trim());
      data.push(
        props.reduce(
          (obj: Partial<T>, key: string, index: number): Partial<T> => {
            obj[key] = cols[index];
            return obj;
          },
          {}
        ) as unknown as T
      );
    }
    return data;
  }
}

export class DataLoader {
  public async loadData<T extends object>(filename: string): Promise<T[]> {
    return new Promise(async (resolve, reject) => {
      const buffer: string[] = [];
      const readStream: fs.ReadStream = fs.createReadStream(filename);
      readStream.once("error", (err) => reject(err));
      const lineProcessor = readline.createInterface({
        input: readStream,
      });
      for await (const line of lineProcessor) {
        buffer.push(line);
      }
      const parser = new CSVParser<T>(buffer);
      resolve(parser.getDataMapped());
    });
  }
}

export interface IRawLocation {
  city: string;
  city_ascii: string;
  state_id: string;
  state_name: string;
  county_fips: string;
  county_name: string;
  lat: string;
  lng: string;
  population: string;
  density: string;
  source: string;
  military: string;
  incorporated: string;
  timezone: string;
  ranking: string;
  zips: string;
  id: string;
}

export class Location
  implements
    Omit<
      IRawLocation,
      | "lng"
      | "lat"
      | "population"
      | "density"
      | "military"
      | "incorporated"
      | "zips"
    >
{
  city: string;
  city_ascii: string;
  state_id: string;
  state_name: string;
  county_fips: string;
  county_name: string;
  lat: number;
  lng: number;
  population: number;
  density: number;
  source: string;
  military: boolean;
  incorporated: boolean;
  timezone: string;
  ranking: string;
  zips: Set<string>;
  id: string;

  closestCity?: Location;

  constructor(rawLocation: IRawLocation) {
    this.city = rawLocation.city;
    this.city_ascii = rawLocation.state_id;
    this.state_id = rawLocation.state_id;
    this.state_name = rawLocation.state_name;
    this.county_fips = rawLocation.county_fips;
    this.county_name = rawLocation.county_name;
    this.lat = Number(rawLocation.lat);
    this.lng = Number(rawLocation.lng);
    this.population = Number(rawLocation.population);
    this.density = Number(rawLocation.density);
    this.source = rawLocation.source;
    this.military = rawLocation.military === "TRUE";
    this.incorporated = rawLocation.incorporated === "TRUE";
    this.timezone = rawLocation.timezone;
    this.ranking = rawLocation.ranking;
    this.zips = new Set(rawLocation.zips.split(" ").map((z) => z.trim()));
    this.id = rawLocation.id;
  }

  public getShortView(): string {
    return `${this.city} (${this.state_name}): ${this.lat}, ${this.lng}, ${this.timezone}`;
  }

  /*
   * Haversine distance
   * https://en.wikipedia.org/wiki/Haversine_formula
   */
  public distanceFrom(city: Location): number {
    const R = 3958.8;
    const rlat1 = this.lat * (Math.PI / 180);
    const rlat2 = city.lat * (Math.PI / 180);
    const difflat = rlat2 - rlat1;
    const difflon = (city.lng - this.lng) * (Math.PI / 180);

    const d =
      2 *
      R *
      Math.asin(
        Math.sqrt(
          Math.sin(difflat / 2) * Math.sin(difflat / 2) +
            Math.cos(rlat1) *
              Math.cos(rlat2) *
              Math.sin(difflon / 2) *
              Math.sin(difflon / 2)
        )
      );
    return d;
  }
}

export type LocationWithDistance = [Location, number];

export class DataProcessor {
  protected referenceBuffer: Location[];
  protected zipLookupTable: Map<string, Location>;

  constructor(rawData: IRawLocation[]) {
    this.zipLookupTable = new Map();
    this.referenceBuffer = rawData.map((raw) => {
      const loc = new Location(raw);
      loc.zips.forEach((zip) => {
        this.zipLookupTable.set(zip, loc);
      });
      return loc;
    });
  }

  public getCityByZIPCode(zipCode: string): Location {
    if (!this.zipLookupTable.has(zipCode)) {
      throw new Error(`Zip code ${zipCode} not found.`);
    }
    return this.zipLookupTable.get(zipCode)!;
  }

  public getClosestCityForZIPCode(zipCode: string): Location {
    const currentCity = this.getCityByZIPCode(zipCode);
    if (currentCity.closestCity) {
      return currentCity.closestCity;
    }
    const cities: LocationWithDistance[] = this.referenceBuffer
      .filter((c) => c.id !== currentCity.id)
      .map((c) => {
        return [c, currentCity.distanceFrom(c)];
      });
    const closestCity = cities.sort(
      (a: LocationWithDistance, b: LocationWithDistance): number => {
        return a[1] - b[1];
      }
    )[0][0];
    currentCity.closestCity = closestCity;
    return closestCity;
  }
}

export class Program {
  public static async main() {
    try {
      const argv = process.argv;
      const zipIndex = argv.findIndex((v) => v === "--zip") + 1;
      const zip = argv[zipIndex];
      if (!zip) {
        throw new Error(
          `No ZIP provided, use "--zip 12345" when running the program.`
        );
      }

      const dataLoader = new DataLoader();
      const rawData: IRawLocation[] = await dataLoader.loadData<IRawLocation>(
        path.join(__dirname, "../simplemaps_uscities_basicv1.79/uscities.csv")
      );
      const processor = new DataProcessor(rawData);

      console.log(`City with zipcode ${zip}:`);
      console.log(processor.getCityByZIPCode(zip).getShortView());

      console.log("");
      console.log(`Closest city:`);
      console.log(processor.getClosestCityForZIPCode(zip).getShortView());
      console.log("");

      process.exit(0);
    } catch (e) {
      console.error(e);
      process.exit(1);
    }
  }
}

if (require.main === module) {
  Program.main();
}
