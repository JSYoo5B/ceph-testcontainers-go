"""Host admission/metadata/package tests. They never build or claim native Ceph proof."""
import contextlib
import copy
import gzip
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import sys
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

NATIVE = Path(__file__).resolve().parents[1]
PROJECT = NATIVE.parents[1]
sys.path.insert(0, str(NATIVE))
import native_build
import native_resume
import debian_runtime

RECIPE = PROJECT / 'image/patches/ceph-20.2.4/recipe.noble-arm64.json'


def assembly_parser():
    path = PROJECT / 'image/slim/debian_packages.py'
    spec = importlib.util.spec_from_file_location('native_fixture_slim_parser', path)
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(path.parent))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


class NativeWorkflowGuards(unittest.TestCase):
    def setUp(self):
        self.recipe = native_build.Recipe(RECIPE)

    def copied_recipe(self, root):
        destination = root / 'inputs'
        shutil.copytree(RECIPE.parent, destination)
        return destination / RECIPE.name

    def test_moved_recipe_has_no_checkout_or_current_directory_dependency(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.copied_recipe(Path(directory))
            moved = native_build.Recipe(path)
            self.assertEqual(moved.inputs, self.recipe.inputs)
            self.assertEqual(moved.cmake, self.recipe.cmake)
            self.assertEqual(moved.targets, self.recipe.targets)
            with mock.patch.object(native_build.subprocess, 'run', side_effect=AssertionError('plan must not execute')), \
                    mock.patch.object(native_build.urllib.request, 'urlopen', side_effect=AssertionError('plan must not fetch')), \
                    contextlib.redirect_stdout(io.StringIO()) as output:
                self.assertEqual(native_build.main(['--plan', '--recipe', str(path)]), 0)
            self.assertEqual(json.loads(output.getvalue())['deb_count'], 22)

    def test_profile_drift_paths_and_changed_manifest_fail_closed(self):
        for changed in [{'architecture': 'amd64'}, {'version': '20.2.5'},
                        {'inputs': {'path': '../outside.json', 'sha256': '0' * 64}}]:
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as directory:
                path = self.copied_recipe(Path(directory))
                data = json.loads(path.read_text()); data.update(changed)
                path.write_text(json.dumps(data))
                with self.assertRaises(RuntimeError):
                    native_build.Recipe(path)
        with tempfile.TemporaryDirectory() as directory:
            path = self.copied_recipe(Path(directory))
            recipe = native_build.Recipe(path)
            recipe.inputs_path.write_text(recipe.inputs_path.read_text() + ' ')
            with self.assertRaisesRegex(RuntimeError, 'SHA256 mismatch'):
                recipe.verify_unchanged()

    def test_recipe_references_cannot_escape_through_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); (root / 'inside').mkdir()
            (root / 'outside').write_text('not an owned input')
            (root / 'inside/link').symlink_to(root / 'outside')
            with self.assertRaisesRegex(RuntimeError, 'escapes its owner'):
                native_build.relative_input(root / 'inside', 'link')

    def test_ordered_patch_hash_tampering_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.copied_recipe(Path(directory)); recipe = native_build.Recipe(path)
            manifest = json.loads(recipe.patch_manifest_path.read_text())
            patch = recipe.patch_manifest_path.parent / manifest['patches'][0]['filename']
            patch.write_bytes(patch.read_bytes() + b'\n')
            with self.assertRaisesRegex(RuntimeError, 'SHA256 mismatch'):
                native_build.patch_inputs(recipe.patch_manifest_path)

    def test_native_recipe_is_the_actual_four_target_low_memory_configuration(self):
        self.assertEqual(self.recipe.targets,
                         ['unittest_rgw_bucket_sync_pipe_rules', 'radosgw', 'radosgw-admin', 'rgw'])
        self.assertEqual(self.recipe.cmake['BOOST_J'], '1')
        self.assertEqual(self.recipe.cmake['CMAKE_CXX_FLAGS_RELEASE'], '-O1 -DNDEBUG -g0')
        self.assertEqual(self.recipe.cmake['CMAKE_EXE_LINKER_FLAGS'], '-fuse-ld=bfd')
        self.assertEqual(self.recipe.cmake['WITH_TESTS'], 'ON')
        self.assertEqual(self.recipe.cmake['WITH_RADOSGW_BEAST_OPENSSL'], 'ON')
        self.assertEqual(self.recipe.data['minimum_gtest_cases'], 9)

    def test_corrupt_cache_or_unapproved_origin_never_replaces_or_downloads(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'archive'; path.write_bytes(b'corrupt')
            expected = {'size': 7, 'sha256': hashlib.sha256(b'correct').hexdigest()}
            with mock.patch.object(native_build.urllib.request, 'urlopen', side_effect=AssertionError('network')):
                with self.assertRaisesRegex(RuntimeError, 'SHA256 mismatch'):
                    native_build.download('https://download.ceph.com/input', path, expected, False)
                with self.assertRaisesRegex(RuntimeError, 'official HTTPS'):
                    native_build.download('https://download.ceph.com.attacker.test/input', path, expected, False)
            self.assertEqual(path.read_bytes(), b'corrupt')

    def test_archive_traversal_and_escaping_symlink_rejected_before_extraction(self):
        for name, link in [('source/../outside', None), ('source/link', '../../../outside')]:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); archive = root / 'source.tar.gz'
                with tarfile.open(archive, 'w:gz') as stream:
                    item = tarfile.TarInfo(name)
                    if link:
                        item.type = tarfile.SYMTYPE; item.linkname = link
                        stream.addfile(item)
                    else:
                        item.size = 1; stream.addfile(item, io.BytesIO(b'x'))
                with self.assertRaisesRegex(RuntimeError, 'Unsafe archive path|symlink escapes'):
                    native_build.extract_verified(archive, root / 'source')
                self.assertFalse((root / 'outside').exists())
                self.assertFalse((root / 'source').exists())

    def test_preparation_cannot_overwrite_prior_native_proof(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); (root / '.ceph-native-selector-run').write_text('schema=1\n')
            proof = '{"schema":1,"native_status":"native-unit-pass","native_gtest_tests":9}\n'
            (root / 'report.json').write_text(proof)
            with mock.patch.object(native_build, 'verify_metadata', side_effect=AssertionError('authentication must not start')):
                with self.assertRaisesRegex(RuntimeError, 'Preserve prior native proof'):
                    native_build.main(['--prepare-only', '--offline', '--recipe', str(RECIPE), '--output', str(root)])
            self.assertEqual((root / 'report.json').read_text(), proof)

    def test_resume_rejects_recipe_identity_change_without_mutating_proof(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); (root / 'source').mkdir(); (root / 'build').mkdir()
            (root / '.ceph-native-selector-run').write_text('schema=1\n')
            report = {'schema': 1, 'native_status': 'configured', 'recipe_sha256': '0' * 64}
            native_build.save_json(root / 'report.json', report)
            original = (root / 'report.json').read_bytes()
            args = SimpleNamespace(recipe_data=self.recipe, patch_manifest=self.recipe.patch_manifest_path)
            patches = native_build.patch_inputs(args.patch_manifest)
            with mock.patch.object(native_build, 'check_linux'):
                with self.assertRaisesRegex(RuntimeError, 'recipe_sha256'):
                    native_resume.admission(args, self.recipe.inputs, root, patches)
            self.assertEqual((root / 'report.json').read_bytes(), original)
            self.assertFalse((root / 'attempts').exists())

    def test_resume_admits_only_declared_linker_transition(self):
        cache = dict(self.recipe.cmake)
        for key in native_resume.LINKER_KEYS:
            cache[key] = '-fuse-ld=lld'
        native_resume.verify_flags(cache, self.recipe.cmake, True)
        with self.assertRaisesRegex(RuntimeError, 'CMake cache differs'):
            native_resume.verify_flags(cache, self.recipe.cmake, False)
        cache.update(self.recipe.cmake)
        native_resume.verify_flags(cache, self.recipe.cmake, False)
        cache['CMAKE_CXX_FLAGS_RELEASE'] = '-O3 -DNDEBUG'
        with self.assertRaisesRegex(RuntimeError, 'CMAKE_CXX_FLAGS_RELEASE'):
            native_resume.verify_flags(cache, self.recipe.cmake, True)

    def test_source_identity_captured_before_patch_commands(self):
        # Exercise source admission with a deliberately stopped orchestration.
        # No mocked operation constitutes successful native build evidence.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); version = self.recipe.inputs['source_commit_reference']
            def extracted(_archive, source):
                (source / 'src').mkdir(parents=True)
                (source / 'src/.git_version').write_text(version + '\n20.2.4\n')
            args = SimpleNamespace(recipe_data=self.recipe)
            report = {'source_archive_path': 'unused-test-input'}
            with mock.patch.object(native_build, 'check_linux'), \
                    mock.patch.object(native_build.shutil, 'which', return_value='test-tool'), \
                    mock.patch.object(native_build, 'extract_verified', side_effect=extracted), \
                    mock.patch.object(native_build, 'check_file'), \
                    mock.patch.object(native_build, 'execute', side_effect=RuntimeError('stop before native patch operation')):
                with self.assertRaisesRegex(RuntimeError, 'stop before native patch operation'):
                    native_build.build_native(args, self.recipe.inputs, root,
                                              native_build.patch_inputs(self.recipe.patch_manifest_path), report)
            self.assertEqual(report['source_manifest_sha256'], native_build.digest(self.recipe.source_manifest_path))
            self.assertEqual(report['archive_git_version'], [version, '20.2.4'])
            self.assertNotIn('native_status', report)

    def test_authenticated_package_relationships_keep_exact_official_versions(self):
        index = native_build.paragraphs((self.recipe.metadata / 'Packages.arm64').read_text())
        controls = {item['Package']: item for item in index
                    if item.get('Version') == self.recipe.data['source_version']}
        parser = assembly_parser(); source_versions = set()
        for package in self.recipe.inputs['runtime_packages']:
            original = controls[package['Package']]
            self.assertEqual(original['Source'], 'ceph')
            source_versions.add(original['Version'])
            if package['Package'] in ('radosgw', 'ceph-common'):
                # Use the authenticated relationship fields; no binary .deb/ELF needed.
                text = '\n'.join(key + ': ' + original[key] for key in
                                 ['Package', 'Version', 'Architecture', 'Source', 'Depends']) + '\n'
                with tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / 'control'; path.write_text(text)
                    debian_runtime.append_control_fields(path, '20.2.4+tc.test', 'a' * 64)
                    changed = native_build.fields(path.read_text())
                    self.assertNotIn('\n\n', path.read_text())
                    for key in ['Package', 'Version', 'Architecture', 'Source']:
                        self.assertEqual(changed[key], original[key])
                    before = parser.parse_relationships(original['Depends'])
                    after = parser.parse_relationships(changed['Depends'])
                    self.assertEqual(after[:-1], before)
                    self.assertEqual(after[-1][0]['name'], 'ceph-tc-native-rgw')
                    self.assertEqual(after[-1][0]['operator'], '=')
        private = native_build.fields(debian_runtime.private_control('20.2.4+tc.test',
                         [{'package': 'libc6', 'version': '2.39'}], self.recipe))
        source_versions.add(private['Source'].split('(', 1)[1].rstrip(')'))
        self.assertEqual(source_versions, {'20.2.4-1noble'})
        base = parser.parse_relationships(controls['ceph-base']['Depends'])
        exact = [v for group in base for v in group if v['name'] == 'ceph-common']
        self.assertEqual(exact[0]['version'], controls['ceph-common']['Version'])

    def test_folded_depends_and_description_preserved_as_one_binary_stanza(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'control'
            path.write_text('Package: radosgw\nVersion: 20.2.4-1noble\nArchitecture: arm64\n'
                            'Depends: ceph-common (= 20.2.4-1noble),\n'
                            ' librados2 (>= 20.2.4), libfoo:any | libbar (>= 2:1.0)\n'
                            'Description: Original description\n keeps its continuation\n')
            debian_runtime.append_control_fields(path, '20.2.4+tc.test', 'a' * 64)
            fields = native_build.fields(path.read_text())
            relations = assembly_parser().parse_relationships(fields['Depends'])
            self.assertEqual(len(relations), 4)
            self.assertEqual(relations[-1][0]['version'], '20.2.4+tc.test')
            self.assertEqual(fields['Description'], 'Original description\nkeeps its continuation')
            self.assertNotIn('\n\n', path.read_text())

    def test_malformed_or_previously_custom_binary_control_is_not_mutated(self):
        for text in ['Package: radosgw\nDepends: libc6,\n',
                     'Package: radosgw\nDepends: libc6\nX-Ceph-TC-Local-Payload: true\n',
                     'Package: radosgw\n\nUnexpected: second paragraph\n']:
            with self.subTest(text=text), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'control'; path.write_text(text)
                with self.assertRaises(RuntimeError):
                    debian_runtime.append_control_fields(path, '20.2.4+tc.test', 'a' * 64)
                self.assertEqual(path.read_text(), text)

    def test_native_not_run_and_tampered_xml_cannot_enter_packaging(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory)
            args = SimpleNamespace(recipe_data=self.recipe, run_dir=run, output=run / 'out',
                                   patch_manifest=self.recipe.patch_manifest_path, deb_directory=None)
            native_build.save_json(run / 'report.json', {'native_status': 'NOT RUN'})
            with mock.patch.object(native_build, 'check_linux'), \
                    mock.patch.object(debian_runtime.shutil, 'which', return_value='test-tool'):
                with self.assertRaisesRegex(RuntimeError, 'nine passed selector gtests'):
                    debian_runtime.package(args)
                # Synthetic admission inputs exercise denial only, never successful proof.
                report = {'native_status': 'native-unit-pass', 'native_gtest_tests': 9,
                          'runtime_status': 'native-artifacts-staged; image/multisite-client proof NOT RUN',
                          'recipe_sha256': self.recipe.sha256,
                          'inputs_manifest_sha256': native_build.digest(self.recipe.inputs_path),
                          'patches_manifest_sha256': native_build.digest(args.patch_manifest),
                          'native_gtest_xml_sha256': '0' * 64}
                native_build.save_json(run / 'report.json', report)
                (run / 'native-gtest.xml').write_text('<testsuites tests="9"/>')
                with self.assertRaisesRegex(RuntimeError, 'SHA256 mismatch'):
                    debian_runtime.package(args)
            self.assertFalse((run / 'out').exists())

    @unittest.skipUnless(shutil.which('gpg') and shutil.which('gpgv'), 'GPG needed for real signature test')
    def test_real_release_signature_and_all_pinned_indexes_verify_without_network(self):
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(native_build.urllib.request, 'urlopen', side_effect=AssertionError('network')):
            proof = native_build.verify_metadata(self.recipe.inputs, Path(directory), self.recipe.metadata)
        self.assertEqual(proof['fingerprint'], '08B73419AC32B4E966C1A330E84AC2C0460F3994')
        self.assertEqual(proof['archive_identity']['sha256'],
                         '99be4f0d00bbaab96c5d43eca8bdbe58c0da22ded27ab3ca6d2d73d70106a005')


if __name__ == '__main__':
    unittest.main()
