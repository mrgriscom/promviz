
REFRESH_INTERVAL = 300;

function init($div, data) {
    var map = L.map($div.attr('id')).setView([30, 0], 2);

    var mapboxLayer = function(tag) {
	return L.tileLayer('http://api.tiles.mapbox.com/v3/' + tag + '/{z}/{x}/{y}.png', {
	    attribution: '<a href="http://www.mapbox.com/about/maps/">MapBox</a>',
	});
    };

    var layers = {
	// TODO: these tags should probably not be hard-coded
	'Map': mapboxLayer('examples.map-uci7ul8p'),
	'Satellite': mapboxLayer('examples.map-qfyrx5r8'), // note: we need a pay account to use this for real
	'Hypso': L.tileLayer('http://maps-for-free.com/layer/relief/z{z}/row{y}/{z}_{x}-{y}.jpg'),
	'Topo': L.tileLayer('http://services.arcgisonline.com/ArcGIS/rest/services/USA_Topo_Maps/MapServer/tile/{z}/{y}/{x}'),
    }
    L.control.layers(layers).addTo(map);
    map.addLayer(layers['Map']);

    L.control.scale().addTo(map);

    loadData(map, data);
}

OVERLAY = null;
function loadData(map, data) {
    var overlay = L.geoJson(data, {
	pointToLayer: function(feature, latlng) {
	    return L.circleMarker(latlng, {
		color: '#000',
		weight: 1.5,
		fillColor: (feature.properties.type == 'summit' ? '#f00' : '#ff0'),
	    });
	},
	onEachFeature: function(feature, layer) {
	    //layer.bindPopup(ago);
	},
    });
    overlay.addTo(map);
    
    bounds = overlay.getBounds();
    map.fitBounds(bounds);

    if (OVERLAY) {
	map.removeLayer(OVERLAY);
    }
    OVERLAY = overlay;
}