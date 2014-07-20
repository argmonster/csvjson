var fs = require("fs"),
	util = require("util");

module.exports = {

	toObject : function(data){
		var content = getContentIfFile(data);
		var hashData = deserialize(content,true, false);
		return outputSave(hashData);
	},

	toArray : function(data){
		var content = getContentIfFile(data);
		var hashData = deserialize(content, false, false);
		return outputSave(hashData);
	},

	toCSV : function(data){
		var content = getContentIfFile(data);
		content = serialize(content);
		return outputSave(content.join("\n")) ;
	},

	toColumnArray: function(data) {
		var content = getContentIfFile(data);
		var hashData = deserialize(content, true, true);
		hashData = pivot(hashData);
		return outputSave(hashData);
	},

	toSchemaObject : function(data){
		var content = getContentIfFile(data);
		var hashData = deserialize(content, true, true);
		return outputSave(hashData);
	}
}

function pivot(data) {
	var out = {};
	data.forEach(function(row) {
		for (key in row) {
			out[key] = out[key] || [];
			out[key].push(row[key]);
		}
	});
	return out;
}

function serialize(content) {
	if(typeof content === "string"){
		content = JSON.parse(content);
	}	
	if(!content.length){
		throw new Error("invalid data");
	}
	var textContent = [],
		headers = false;
	content.forEach(function(item){
		if(util.isArray(item)){
			textContent.push(item.join(','));
		}else{
			headers = Object.keys(item).join(',');
			var data = [];
			for(var i in item){
				data.push(item[i]);
			}
			textContent.push(data.join(','));
		}
	});
	if(headers){
		textContent.unshift(headers);
	}
	return textContent;
}

function deserialize(content, hasheaders, hasschema) {
	content = content.split(/[\n\r]+/ig);
	var headers = [], hashData = []; //, hashItem;
	if (hasheaders) {
		headers = processHeaders(content);
	} 
	content.forEach(function(item){
		if(item){
			item = item.split(/\s*,\s*/);
			var hashItem = hasheaders ? {} : [];
			var llength = headers.length || item.length;
			for (var index = 0; index < llength; index++) {
				putDataInSchema(headers[index] || index, item[index], hashItem,hasschema);
			}
			hashData.push(hashItem);
		}
	});
	return hashData;
}

function processHeaders(data) { 
	return data.shift().split(new RegExp(/\s*,\s*/));
}

function putDataInSchema(header, item, schema, useschema){
	var match = header.toString().match(/\.|\[\]|-|\+/ig);
	if(match && useschema){
		if(match.indexOf('-') !== -1){
			return true;
		}else if(match.indexOf('.') !== -1){
			var headParts = header.split('.');
			var currentPoint = headParts.shift();
			schema = schema || {};
			schema[currentPoint] = schema[currentPoint] || {};
			putDataInSchema(headParts.join("."), item, schema[currentPoint]);
		}else if(match.indexOf('[]') !== -1){
			var headerName = header.replace(/\[\]/ig,"");
			if(!schema[headerName]){
				schema[headerName] = [];
			}
			schema[headerName].push(item);	
		}else if(match.indexOf('+') !== -1){
			var headerName = header.replace(/\+/ig,"");
			item = trimQuote(item);
			if (item.toString().length) {
				schema[headerName] = Number(item);
			} else { 
				schema[headerName] = null;
			}
		}
	}else{
		schema[header] = item !== undefined ? trimQuote(item.toString()) : null;
	}
	return schema ;
}

function getContentIfFile(filepath){
	if (fs.existsSync(filepath)) {
        return fs.readFileSync(filepath, 'utf8');
    }
	throw new Error("invalid file path");
}


function outputSave(data){
	return {
		output : data,
		save : function(filepath){
			if(typeof data === "object"){
				data = JSON.stringify(data);
			}
			fs.writeFileSync(filepath, data, {encoding:'utf8'});
			return this;
		}
	}
}

function trimQuote(str){
	return str.toString().trim().replace(/^["|'](.*)["|']$/, '$1');
}
