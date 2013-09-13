
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
        'Map': mapboxLayer('examples.map-9ijuk24y'),
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

    setTimeout(function() {
        loadData(map, data);
    }, 50);
}

OVERLAY = null;
function loadData(map, data) {
    var overlay = L.geoJson(data, {
        pointToLayer: function(feature, latlng) {
            if (MODE == 'single') {
                return L.circleMarker(latlng, {
                    color: '#000',
                    weight: 1.5,
                    fillColor: (feature.properties.type == 'summit' ? '#f00' : '#ff0'),
                });
            } else {
                var color;
                if (feature.properties.prom_ft > 4000) {
                    color = 'hsl(0, 100%, 50%)';
                } else if (feature.properties.prom_ft > 3000) {
                    color = 'hsl(40, 100%, 50%)';
                } else if (feature.properties.prom_ft > 2000) {
                    color = 'hsl(80, 100%, 50%)';
                } else if (feature.properties.prom_ft > 1500) {
                    color = 'hsl(120, 100%, 50%)';
                } else if (feature.properties.prom_ft > 1000) {
                    color = 'hsl(160, 100%, 50%)';
                } else if (feature.properties.prom_ft > 5000) {
                    color = 'hsl(200, 100%, 50%)';
                } else {
                    color = 'hsl(240, 100%, 50%)';
                }
                return L.circleMarker(latlng, {
                    radius: 10. * Math.sqrt(feature.properties.prom_ft / 500.),
                    fillColor: color,
                    fillOpacity: .5,
                });
            }
        },
        onEachFeature: function(feature, layer) {
            var props = feature.properties
            if (MODE == 'single') {                
                var $div = $('<div>');
                if (props.type == 'summit') {
                    $div.html('<div>' + round(props.prom_ft, 1) + (props.min_bound ? '*' : '') + '</div><div>' + round(props.elev_ft, 1) + '</div>')
                } else if (props.type == 'saddle') {
                    $div.html('<div>' + round(props.elev_ft, 1) + '</div>');
                } else {
                    return;
                }
                layer.bindPopup($div[0]);
            } else {
                var $div = $('<div>');
                $div.html('<div><a target="_blank" href="/view/prom' + props.geo + '.geojson">' + props.geo + '</a></div><div>' + round(props.prom_ft, 1) + '</div><div>' + round(props.elev_ft, 1) + '</div>');
                layer.bindPopup($div[0]);
            }
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

function round(x, digits) {
    return Math.round(x * Math.pow(10., digits)) * Math.pow(10., -digits);
}