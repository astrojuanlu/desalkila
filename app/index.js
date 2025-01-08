import { MapboxOverlay } from "@deck.gl/mapbox";
import { GeoJsonLayer } from "@deck.gl/layers";
import maplibregl from "maplibre-gl";
import { asyncBufferFromUrl } from "hyparquet";
import { toGeoJson } from "geoparquet";

async function initializeMap() {
  try {
    // Load and convert GeoParquet file
    const url = "https://raw.githubusercontent.com/astrojuanlu/desalkila/refs/heads/app/app/public/registry_cam_no_vuts_simple.geoparquet";
    const data = await asyncBufferFromUrl({ url });
    console.log(data);
    const geojson = await toGeoJson({ data });
    console.log(geojson);

    const map = new maplibregl.Map({
      container: "map",
      style: {
        version: 8,
        sources: {
          osm: {
            type: "raster",
            tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
            tileSize: 256,
            attribution: "&copy; OpenStreetMap Contributors",
            maxzoom: 19,
          },
        },
        layers: [
          {
            id: "osm",
            type: "raster",
            source: "osm",
            minzoom: 0,
            maxzoom: 19,
          },
        ],
      },
      center: [-3.7038, 40.4168], // Madrid
      zoom: 11,
    });

    map.on("load", () => {
      // Initialize your deck instance
      const deckOverlay = new MapboxOverlay({
        layers: [
          new GeoJsonLayer({
            id: "geojson-layer",
            data: geojson,
            filled: true,
            pointRadiusMinPixels: 8,
            pointRadiusScale: 2000,
            getPointRadius: 1,
            getFillColor: [255, 0, 0, 200], // Red dots
            pickable: true,
            onHover: ({ object }) => {
              if (object) {
                console.log(object.properties.name);
              }
            },
          }),
        ],
      });

      map.addControl(deckOverlay);
      map.addControl(new maplibregl.NavigationControl());
    });
  } catch (error) {
    console.error("Error initializing map:", error);
  }
}

initializeMap();
