
REFRESH_INTERVAL = 300;

function init($div, data) {
    var map = L.map($div.attr('id')).setView([30, 0], 2);

    var mapboxLayer = function(tag) {
        return L.tileLayer('http://api.tiles.mapbox.com/v3/' + tag + '/{z}/{x}/{y}.png');
    };

    var layers = {
        'Hypso': L.tileLayer('http://s3.amazonaws.com/oilslick/{z}/{x}/{y}.jpg', {maxZoom: 11}),
        'Hypso-old': L.tileLayer('http://maps-for-free.com/layer/relief/z{z}/row{y}/{z}_{x}-{y}.jpg', {maxZoom: 11}),
        'Topo': L.tileLayer('http://services.arcgisonline.com/ArcGIS/rest/services/USA_Topo_Maps/MapServer/tile/{z}/{y}/{x}', {maxZoom: 15}),
        'Satellite': mapboxLayer('examples.map-qfyrx5r8'),
        'Terrain': L.tileLayer('http://mt{s}.google.com/vt/lyrs=p&x={x}&y={y}&z={z}', {subdomains: '0123'}),
    }
    var layerOrder = ['Hypso', 'Hypso-old', 'Terrain', 'Topo', 'Satellite'];
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
        style: function(feature) {
            var props = feature.properties;
            if (props.type == 'divide') {
                return {color: '#00f'};
            } else if (props.type == 'toparent') {
                return {color: '#f00'};
            } else if (props.type == 'domain') {
                return {color: '#0f0'};
            }
        },
        pointToLayer: function(feature, latlng) {
            if (MODE == 'single') {
                var props = feature.properties;
                var style;
                if (props.type == 'peak' || props.type == 'pit') {
                    style = {fillColor: '#f00'};
                } else if (props.type == 'saddle') {
                    style = {fillColor: '#ff0'};
                } else if (props.type == 'threshold') {
                    style = {fillColor: '#00f', radius: 5};
                } else if (props.type == 'child') {
                    style = {fillColor: '#0ff', radius: 7.5};
                } else if (props.type == 'parent') {
                    style = {fillColor: '#0f0'};
                }
                style.color = '#000';
                style.weight = 1.5;
                return L.circleMarker(latlng, style);
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
                var self = (props.type == 'peak' || props.type == 'pit');
                if (self || props.type == 'parent' || props.type == 'child') {
                    var title = props.name || props.geo;
                    if (!self) {
                        title = '<a target="_blank" href="/view/' + props.geo + '">' + title + '</a>';
                    }
                    var html = '<div>' + title + '</div>';
                    if (props.type == 'child') {
                        html += '<div>#' + props.ix + '</div>';
                    }
                    html += '<div>' + props.prom_ft.toFixed(1) + (props.min_bound ? '*' : '') + '</div><div>' + props.elev_ft.toFixed(1) + '</div>';
                    if (self) {
                        html += '<hr><table style="font-size: 12px;">';
                        _.each(DATA.features, function(e) {
                            var p = e.properties;
                            if (p.type == 'child') {
                                html += '<tr><td>' + p.ix + '</td><td><a target="_blank" href="/view/' + p.geo + '">' + (p.name || p.geo) + '</a></td><td>' + p.prom_ft.toFixed(1) + '</td></tr>';
                            }
                        });
                        html += '</table>';
                    }
                    $div.html(html);
                } else if (props.type == 'saddle') {
                    $div.html('<div>' + (props.name || '') + '</div><div>' + props.elev_ft.toFixed(1) + '</div>');
                } else {
                    return;
                }
                layer.bindPopup($div[0]);
            } else {
                var $div = $('<div>');
                $div.html('<div><a target="_blank" href="/view/' + props.geo + '">' + (props.name || props.geo) + '</a></div><div>' + props.prom_ft.toFixed(1) + (props.min_bound ? '*' : '') + '</div><div>' + props.elev_ft.toFixed(1) + '</div>');
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