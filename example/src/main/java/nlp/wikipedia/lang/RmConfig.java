/**
 * This file is part of Wikiforia.
 *
 * Wikiforia is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * Wikiforia is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Wikiforia. If not, see <http://www.gnu.org/licenses/>.
 */
 package nlp.wikipedia.lang;

//Autogenerated from Wikimedia sources at 2015-04-16T13:55:11+00:00

public class RmConfig extends TemplateConfig {
	public RmConfig() {
		addNamespaceAlias(-2, "Multimedia");
		addNamespaceAlias(-1, "Spezial");
		addNamespaceAlias(1, "Discussiun");
		addNamespaceAlias(2, "Utilisader");
		addNamespaceAlias(3, "Utilisader_discussiun");
		addNamespaceAlias(5, "Wikipedia_discussiun");
		addNamespaceAlias(6, "Datoteca");
		addNamespaceAlias(7, "Datoteca_discussiun");
		addNamespaceAlias(8, "MediaWiki");
		addNamespaceAlias(9, "MediaWiki_discussiun");
		addNamespaceAlias(10, "Model");
		addNamespaceAlias(11, "Model_discussiun");
		addNamespaceAlias(12, "Agid");
		addNamespaceAlias(13, "Agid_discussiun");
		addNamespaceAlias(14, "Categoria");
		addNamespaceAlias(15, "Categoria_discussiun");

		addI18nCIAlias("redirect", "#RENVIAMENT", "#REDIRECT");
		addI18nAlias("img_thumbnail", "miniatura", "thumbnail", "thumb");
		addI18nAlias("img_manualthumb", "miniatura=$1", "thumbnail=$1", "thumb=$1");
		addI18nAlias("img_upright", "sidretg", "sidretg=$1", "sidretg_$1", "upright", "upright=$1", "upright $1");
	}

	@Override
	protected String getSiteName() {
		return "Wikipedia";
	}

	@Override
	protected String getWikiUrl() {
		return "http://rm.wikipedia.org/";
	}

	@Override
	public String getIso639() {
		return "rm";
	}
}