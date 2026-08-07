import { MapboxOverlay } from "@deck.gl/mapbox";
import { GeoJsonLayer } from "@deck.gl/layers";
import maplibregl from "maplibre-gl";
import { asyncBufferFromUrl, parquetMetadataAsync } from "hyparquet";
import { toGeoJson } from "geoparquet";
import proj4 from "proj4";
import Converter from "projjson-to-wkt";

async function getSourceCrs(file) {
  const metadata = await parquetMetadataAsync(file);
  const geoMetadata = JSON.parse(
    metadata.key_value_metadata?.find((kv) => kv.key === "geo").value,
  );
  return Converter.toWkt1(geoMetadata.columns.geometry.crs);
}

async function initializeMap() {
  try {
    // Load and convert GeoParquet file
    const url =
      "https://raw.githubusercontent.com/astrojuanlu/desalkila/refs/heads/app/app/public/registry_cam_no_vuts_simple.geoparquet";
    const file = await asyncBufferFromUrl({ url });
    const sourceCrs = await getSourceCrs(file);

    var geojson = await toGeoJson({ file });
    geojson = {
      type: "FeatureCollection",
      features: geojson.features.map((feature) => {
        const reprojectedGeometry = {
          type: feature.geometry.type,
          coordinates: proj4(
            sourceCrs,
            "EPSG:4326",
            feature.geometry.coordinates,
          ),
        };
        return { ...feature, geometry: reprojectedGeometry };
      }),
    };

    const map = new maplibregl.Map({
      container: "map",
      style: 'https://tiles.openfreemap.org/styles/bright',
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
            pointRadiusMinPixels: 4,
            getPointRadius: 1,
            getFillColor: [235, 147, 96, 200],
            getLineColor: [171, 102, 62],
            pickable: true,
            onHover: ({ object }) => {
              if (object) {
                console.log(object.properties);
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
