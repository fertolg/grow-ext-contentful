import contentful
from grow.common import utils
from protorpc import messages
import grow
import os


class KeyMessage(messages.Message):
    preview = messages.StringField(1)
    production = messages.StringField(2)


class BindingMessage(messages.Message):
    collection = messages.StringField(1)
    contentModel = messages.StringField(2)


class ContentfulPreprocessor(grow.Preprocessor):
    KIND = 'contentful'
    _edit_entry_url_format = 'https://app.contentful.com/spaces/{space}/entries/{entry}'
    _edit_space_url_format = 'https://app.contentful.com/spaces/{space}/entries'
    _cdn_endpoint = 'cdn.contentful.com'
    _preview_endpoint = 'preview.contentful.com'

    class Config(messages.Message):
        space = messages.StringField(2)
        keys = messages.MessageField(KeyMessage, 3)
        bind = messages.MessageField(BindingMessage, 4, repeated=True)

    def _parse_field(self, field):
        if isinstance(field, contentful.Asset):
            return field.url()
        elif isinstance(field, contentful.Entry):
            fields, body, basename = self._parse_entry(field, doc_fields=False)
            return fields
        elif isinstance(field, contentful.Link):
            return self._parse_field(field.resolve(self.client))
        elif isinstance(field, list):
            return [self._parse_field(sub_field) for sub_field in field]
        return field

    def _parse_entry(self, entry, doc_fields=True):
        """Parses an entry from Contentful."""
        locales = self.locales
        entry_fields = entry.fields()
        content_type = next(content_type
                            for content_type
                            in self.content_types
                            if content_type.id == entry.content_type.id)
        parsed_fields = {}
        body = entry_fields.pop('body', None)
        for key, field in entry_fields.iteritems():
            field_localized = next((field_schema.localized for field_schema in content_type.fields if field_schema.id == key), False)
            key = self._sanitize_key(key) if doc_fields else key
            if field_localized:
                key = '{}@'.format(key)
            parsed_fields[key] = self._parse_field(field)
        for locale in locales:
            for key, field in entry.fields(locale).iteritems():
                key = self._sanitize_key(key) if doc_fields else key
                key = '{}@{}'.format(key, locale)
                parsed_fields[key] = self._parse_field(field)
        if body:
            body = body
            ext = 'md'
        else:
            body = ''
            ext = 'yaml'
        basename = '{}.{}'.format(entry.sys['id'], ext)
        if isinstance(body, unicode):
            body = body.encode('utf-8')
        return parsed_fields, body, basename

    def _sanitize_key(self, key):
        if key == 'title':
            return '$title'
        if key == 'slug':
            return '$slug'
        if key == 'category':
            return '$category'
        return key

    def bind_collection(self, collection_pod_path, contentful_model):
        """Binds a Grow collection to a Contentful collection."""
        entries = self.client.entries({
            'content_type': contentful_model,
            'locale': '*',
            'include': 10
        })
        collection = self.pod.get_collection(collection_pod_path)
        existing_pod_paths = [
            doc.pod_path for doc in collection.list_docs(recursive=False, inject=False)]
        new_pod_paths = []
        for i, entry in enumerate(entries):
            fields, body, basename = self._parse_entry(entry)
            # TODO: Ensure `create_doc` doesn't die if the file doesn't exist.
            path = os.path.join(collection.pod_path, basename)
            if not self.pod.file_exists(path):
                self.pod.write_yaml(path, {})
            doc = collection.create_doc(basename, fields=fields, body=body)
            new_pod_paths.append(doc.pod_path)
            self.pod.logger.info('Saved -> {}'.format(doc.pod_path))
        pod_paths_to_delete = set(existing_pod_paths) - set(new_pod_paths)
        for pod_path in pod_paths_to_delete:
            self.pod.delete_file(pod_path)
            self.pod.logger.info('Deleted -> {}'.format(pod_path))

    def run(self, *args, **kwargs):
        self.content_types = self.client.content_types()
        self.locales = [locale.code for locale in self.client.space().locales if not locale.default]
        for binding in self.config.bind:
            self.bind_collection(binding.collection,
                                 binding.contentModel)

    @property
    @utils.memoize
    def client(self):
        """Contentful API client."""
        endpoint = None
        token = self.config.keys.production
        endpoint = ContentfulPreprocessor._cdn_endpoint
        # Use preview endpoint if preview key is provided.
        if self.config.keys.preview:
            token = self.config.keys.preview
            endpoint = ContentfulPreprocessor._preview_endpoint
        return contentful.Client(
            self.config.space,
            token,
            api_url=endpoint,
            default_locale='zh'
        )

    def can_inject(self, doc=None, collection=None):
        if not self.injected:
            return False
        for binding in self.config.bind:
            if doc and doc.pod_path.startswith(binding.collection):
                return True
            if (collection and
                    self._normalize_path(collection.pod_path)
                    == self._normalize_path(binding.collection)):
                return True
        return False

    def inject(self, doc=None, collection=None):
        """Conditionally injects data into documents or a collection, without
        updating the filesystem. If doc is provided, the document's fields are
        injected. If collection is provided, returns a list of injected
        document instances."""
        if doc is not None:
            entry = self.client.entry(doc.base, {'locale': '*', 'include': 10})
            if not entry:
                self.pod.logger.info('Contentful entry not found: {}'.format(doc.base))
                return  # Corresponding doc not found in Contentful.
            fields, body, basename = self._parse_entry(entry)
            if isinstance(body, unicode):
                body = body.encode('utf-8')
            doc.inject(fields=fields, body=body)
            return doc
        elif collection is not None:
            entries = self.client.entries({
                'content_type': contentful_model,
                'locale': '*',
                'include': 10
            })
            docs = []
            for binding in self.config.bind:
                if (self._normalize_path(collection.pod_path)
                        != self._normalize_path(binding.collection)):
                    continue
                docs += self.create_doc_instances(
                    entries, collection, binding.contentModel)
            return docs

    def create_doc_instances(self, entries, collection, contentful_model):
        docs = []
        for i, entry in enumerate(entries):
            fields, body, basename = self._parse_entry(entry)
            pod_path = os.path.join(collection.pod_path, basename)
            doc = collection.get_doc(pod_path)
            doc.inject(fields=fields, body=body)
            docs.append(doc)
        return docs

    def _normalize_path(self, path):
        """Normalizes a collection path."""
        return path.rstrip('/')

    def get_edit_url(self, doc=None):
        """Returns the URL to edit in Contentful."""
        if doc:
            return ContentfulPreprocessor._edit_entry_url_format.format(
                space=self.config.space, entry=doc.base)
        return ContentfulPreprocessor._edit_space_url_format.format(
            space=self.config.space)
