
REFRESH_INTERVAL = 300;

function init($div, data) {
    var map = L.map($div.attr('id')).setView([30, 0], 2);

    var mapboxLayer = function(tag) {
	return L.tileLayer('http://api.tiles.mapbox.com/v3/' + tag + '/{z}/{x}/{y}.png', {
	    attribution: '<a href="http://www.mapbox.com/about/maps/">MapBox</a>',
	});
    };

    var layers = {
	'Hypso': L.tileLayer('http://maps-for-free.com/layer/relief/z{z}/row{y}/{z}_{x}-{y}.jpg', {maxZoom: 11}),
	'Map': mapboxLayer('examples.map-uci7ul8p'),
	'Topo': L.tileLayer('http://services.arcgisonline.com/ArcGIS/rest/services/USA_Topo_Maps/MapServer/tile/{z}/{y}/{x}', {maxZoom: 15}),
	'Satellite': mapboxLayer('examples.map-qfyrx5r8'),
    }
    var layerOrder = ['Hypso', 'Topo', 'Map', 'Satellite'];
    L.control.layers(layers).addTo(map);

    var activeLayer = null;
    map.on('baselayerchange', function(e) {
	activeLayer = e.layer;
    });

    var setLayer = function(tag) {
	if (activeLayer != null) {
	    map.removeLayer(activeLayer);
	}
	map.addLayer(layers[tag]);
    };

    setLayer('Hypso');

    L.control.scale().addTo(map);

    $(document).keydown(function(e) {
	if (e.keyCode == 76) { // 'l'
	    if (activeLayer) {
		var tag = null;
		$.each(layers, function(k, v) {
		    if (activeLayer == v) {
			tag = k;
			return false;
		    }
		});
		var next = layerOrder[(layerOrder.indexOf(tag) + 1) % layerOrder.length];
		setLayer(next);
	    }
	}
    });

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