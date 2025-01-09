import { MapboxOverlay } from "@deck.gl/mapbox";
import { GeoArrowScatterplotLayer } from "@geoarrow/deck.gl-layers";
import maplibregl from "maplibre-gl";
import { tableFromIPC } from "apache-arrow";
import { readGeoParquet } from "@geoarrow/geoparquet-wasm";
import Converter from "projjson-to-wkt";
// import proj4 from "proj4";

async function getSourceCrs(table) {
  const geoMetadata = JSON.parse(table.schema.metadata.get("geo"));
  return Converter.toWkt1(geoMetadata.columns.geometry.crs);
}

async function loadGeoParquet(url) {
  const resp = await fetch(url);
  console.log(resp);
  const arrayBuffer = await resp.arrayBuffer();
  console.log(arrayBuffer);
  const data = new Uint8Array(arrayBuffer);
  console.log(data)
  const wasmTable = readGeoParquet(data);
  console.log(wasmTable);
  return tableFromIPC(wasmTable.intoIPCStream());
}

async function initializeMap() {
  console.log("Starting");

  // Load and convert GeoParquet file
  const url =
    "https://raw.githubusercontent.com/astrojuanlu/desalkila/refs/heads/app/app/public/registry_cam_no_vuts_simple.geoparquet";
  const table = await loadGeoParquet(url);
  console.log(table);
  const sourceCrs = await getSourceCrs(table);
  console.log(sourceCrs);

  const map = new maplibregl.Map({
    container: "map",
    style: "https://tiles.openfreemap.org/styles/bright",
    center: [-3.7038, 40.4168], // Madrid
    zoom: 11,
  });
  console.log(map);

  map.on("load", () => {
    // Initialize your deck instance
    const deckOverlay = new MapboxOverlay({
      layers: [
        new GeoArrowScatterplotLayer({
          id: "geojson-layer",
          data: table,
          filled: true,
          getPosition: table.getChild("geometry"),
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
}

initializeMap();
